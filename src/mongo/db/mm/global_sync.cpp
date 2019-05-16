/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include <vector>

#include "mongo/db/mm/global_fetcher.h"
#include "mongo/db/mm/global_sync.h"

#include "mongo/base/counter.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/connection_pool.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/concurrency/replication_state_transition_lock_guard.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/logical_clock.h"
#include "mongo/db/ops/write_ops.h"
#include "mongo/db/repl/data_replicator_external_state_impl.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/oplog_interface_local.h"
#include "mongo/db/repl/oplog_interface_remote.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/replication_coordinator_impl.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/rollback_source_impl.h"
#include "mongo/db/repl/rs_rollback.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/s/shard_identity_rollback_notifier.h"
#include "mongo/db/server_parameters.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/rpc/metadata/repl_set_metadata.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/time_support.h"

namespace mongo {

using std::string;

namespace repl {

namespace {
const int kSleepToAllowBatchingMillis = 2;
const int kSmallBatchLimitBytes = 40000;
const Milliseconds kRollbackOplogSocketTimeout(10 * 60 * 1000);
// 16MB max batch size / 12 byte min doc size * 10 (for good measure) = defaultBatchSize to use.
const auto defaultBatchSize = (16 * 1024 * 1024) / 12 * 10;
constexpr int defaultRollbackBatchSize = 2000;

// If 'forceRollbackViaRefetch' is true, always perform rollbacks via the refetch algorithm, even if
// the storage engine supports rollback via recover to timestamp.
constexpr bool forceRollbackViaRefetchByDefault = false;

/*
static int CurrentSyncSourcePort = 20017;

void computeCurrentSyncSourcePort() {
    static int cur = -1;

    if (isMongodWithGlobalSync.load()) {
        CurrentSyncSourcePort = mmPortConfig.load();
        log() << "MultiMaster Computed CurrentSyncSource for MM Shard: " << CurrentSyncSourcePort;
        return;
    }

    ++cur;

    invariant(isMongoG.load());

    switch (cur % 3) {
        case 0:
            CurrentSyncSourcePort = mmPort1.load();
            break;

        case 1:
            CurrentSyncSourcePort = mmPort2.load();
            break;

        case 2:
            CurrentSyncSourcePort = mmPort3.load();
            break;
    }
    log() << "MultiMaster Computed CurrentSyncSource " << CurrentSyncSourcePort;
}
std::string computeInstanceId() {
    std::string hostName = getHostNameCached();
    return str::stream() << hostName << ":" << CurrentSyncSourcePort;
}
*/
/**
 * Extends DataReplicatorExternalStateImpl to be member state aware.
 */
class DataReplicatorExternalStateGlobalSync : public DataReplicatorExternalStateImpl {
public:
    DataReplicatorExternalStateGlobalSync(
        ReplicationCoordinator* replicationCoordinator,
        ReplicationCoordinatorExternalState* replicationCoordinatorExternalState,
        GlobalSync* globalSync);
    bool shouldStopFetching(const HostAndPort& source,
                            const rpc::ReplSetMetadata& replMetadata,
                            boost::optional<rpc::OplogQueryMetadata> oqMetadata) override;

private:
    GlobalSync* _globalSync;
};

DataReplicatorExternalStateGlobalSync::DataReplicatorExternalStateGlobalSync(
    ReplicationCoordinator* replicationCoordinator,
    ReplicationCoordinatorExternalState* replicationCoordinatorExternalState,
    GlobalSync* globalSync)
    : DataReplicatorExternalStateImpl(replicationCoordinator, replicationCoordinatorExternalState),
      _globalSync(globalSync) {}

bool DataReplicatorExternalStateGlobalSync::shouldStopFetching(
    const HostAndPort& source,
    const rpc::ReplSetMetadata& replMetadata,
    boost::optional<rpc::OplogQueryMetadata> oqMetadata) {
    return _globalSync->shouldStopFetching();
    // return DataReplicatorExternalStateImpl::shouldStopFetching(source, replMetadata, oqMetadata);
}

size_t getSize(const BSONObj& o) {
    // SERVER-9808 Avoid Fortify complaint about implicit signed->unsigned conversion
    return static_cast<size_t>(o.objsize());
}
}  // namespace

GlobalSync::GlobalSync(ReplicationCoordinator* replicationCoordinator,
                       ReplicationCoordinatorExternalState* replicationCoordinatorExternalState,
                       HostAndPort syncSource,
                       const std::string& instanceId,
                       bool isMongodWithGlobalSync,
                       bool isMongoG,
                       OpTime lastSynced)
    : _replCoord(replicationCoordinator),
      _replicationCoordinatorExternalState(replicationCoordinatorExternalState),
      _syncSourceHost(syncSource),
      _instanceId(instanceId),
      _isMongodWithGlobalSync(isMongodWithGlobalSync),
      _isMongoG(isMongoG),
      _lastOpTimeFetched(lastSynced) {}

void GlobalSync::startup() {
    invariant(!_producerThread);
    _producerThread.reset(new stdx::thread([this] { _run(); }));
}

void GlobalSync::shutdown() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    _state = ProducerState::Stopped;

    if (_syncSourceResolver) {
        _syncSourceResolver->shutdown();
    }

    if (_oplogFetcher) {
        _oplogFetcher->shutdown();
    }

    if (_rollback) {
        _rollback->shutdown();
    }

    _inShutdown = true;
}

void GlobalSync::join() {
    _producerThread->join();
}

bool GlobalSync::inShutdown() const {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _inShutdown_inlock();
}

bool GlobalSync::_inShutdown_inlock() const {
    return _inShutdown;
}

void GlobalSync::_run() {
    Client::initThread("GlobalSync");
    AuthorizationSession::get(cc())->grantInternalAuthorization();

    while (!inShutdown()) {
        try {
            _runProducer();
        } catch (const DBException& e) {
            std::string msg(str::stream() << "sync producer problem: " << redact(e));
            error() << msg;
            sleepmillis(100);  // sleep a bit to keep from hammering this thread with temp. errors.
        } catch (const std::exception& e2) {
            // redact(std::exception&) doesn't work
            severe() << "sync producer exception: " << redact(e2.what());
            fassertFailed(51090);
        }
    }
    // No need to reset optimes here because we are shutting down.
    stop(false);
}

void GlobalSync::_runProducer() {
    if (getState() == ProducerState::Stopped) {
        sleepsecs(1);
        return;
    }

    /*
    auto memberState = _replCoord->getMemberState();
    invariant(!memberState.rollback());
    invariant(!memberState.startup());

    // We need to wait until initial sync has started.
    if (_replCoord->getMyLastAppliedOpTime().isNull()) {
        sleepsecs(1);
        return;
    }
    */
    // we want to start when we're no longer primary
    // start() also loads _lastOpTimeFetched, which we know is set from the "if"
    {
        auto opCtx = cc().makeOperationContext();
        if (getState() == ProducerState::Starting) {
            start(opCtx.get());
            log() << "MultiMaster _runProducer starting lastOpTimeFetched: " << _lastOpTimeFetched;
        }
        // POC hack
        // computeCurrentSyncSourcePort();
    }
    _produce();
}

void GlobalSync::_produce() {
    if (MONGO_FAIL_POINT(stopReplProducer)) {
        // This log output is used in js tests so please leave it.
        log() << "globalSync - stopReplProducer fail point "
                 "enabled. Blocking until fail point is disabled.";

        // TODO(SERVER-27120): Remove the return statement and uncomment the while loop.
        // Currently we cannot block here or we prevent primaries from being fully elected since
        // we'll never call _signalNoNewDataForApplier.
        //        while (MONGO_FAIL_POINT(stopReplProducer) && !inShutdown()) {
        //            mongo::sleepsecs(1);
        //        }
        mongo::sleepsecs(1);
        return;
    }

    // _syncSourceHost = HostAndPort("localhost", CurrentSyncSourcePort);
    // this oplog reader does not do a handshake because we don't want the server it's syncing
    // from to track how far it has synced
    HostAndPort oldSource;
    OpTime lastOpTimeFetched;
    HostAndPort source;

    /* POC
    SyncSourceResolverResponse syncSourceResp;
    {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        if (_lastOpTimeFetched.isNull()) {
            // then we're initial syncing and we're still waiting for this to be set
            lock.unlock();
            sleepsecs(1);
            // if there is no one to sync from
            return;
        }

        if (_state != ProducerState::Running) {
            return;
        }

        oldSource = _syncSourceHost;
    }

    // find a target to sync from the last optime fetched
    {
        OpTime minValidSaved;
        {
            auto opCtx = cc().makeOperationContext();
            minValidSaved = _replicationProcess->getConsistencyMarkers()->getMinValid(opCtx.get());
        }
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        if (_state != ProducerState::Running) {
            return;
        }
        const auto requiredOpTime = (minValidSaved > _lastOpTimeFetched) ? minValidSaved : OpTime();
        lastOpTimeFetched = _lastOpTimeFetched;
        if (!_syncSourceHost.empty()) {
            log() << "Clearing sync source " << _syncSourceHost << " to choose a new one.";
        }
        _syncSourceHost = HostAndPort();
        _syncSourceResolver = stdx::make_unique<SyncSourceResolver>(
            _replicationCoordinatorExternalState->getTaskExecutor(),
            _replCoord,
            lastOpTimeFetched,
            requiredOpTime,
            [&syncSourceResp](const SyncSourceResolverResponse& resp) { syncSourceResp = resp; });
    }
    // This may deadlock if called inside the mutex because SyncSourceResolver::startup() calls
    // ReplicationCoordinator::chooseNewSyncSource(). ReplicationCoordinatorImpl's mutex has to
    // acquired before GlobalSync's.
    // It is safe to call startup() outside the mutex on this instance of SyncSourceResolver because
    // we do not destroy this instance outside of this function which is only called from a single
    // thread.
    auto status = _syncSourceResolver->startup();
    if (ErrorCodes::CallbackCanceled == status || ErrorCodes::isShutdownError(status.code())) {
        return;
    }
    fassert(51094, status);
    _syncSourceResolver->join();
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _syncSourceResolver.reset();
    }

    if (syncSourceResp.syncSourceStatus == ErrorCodes::OplogStartMissing) {
        // All (accessible) sync sources were too stale.
        if (_replCoord->getMemberState().primary()) {
            warning() << "Too stale to catch up.";
            log() << "Our newest OpTime : " << lastOpTimeFetched;
            log() << "Earliest OpTime available is " << syncSourceResp.earliestOpTimeSeen
                  << " from " << syncSourceResp.getSyncSource();
            _replCoord->abortCatchupIfNeeded().transitional_ignore();
            return;
        }

        // We only need to mark ourselves as too stale once.
        if (_tooStale) {
            return;
        }

        // Mark yourself as too stale.
        _tooStale = true;

        // Need to take the RSTL in mode X to transition out of SECONDARY.
        auto opCtx = cc().makeOperationContext();
        ReplicationStateTransitionLockGuard transitionGuard(opCtx.get());

        error() << "too stale to catch up -- entering maintenance mode";
        log() << "Our newest OpTime : " << lastOpTimeFetched;
        log() << "Earliest OpTime available is " << syncSourceResp.earliestOpTimeSeen;
        log() << "See http://dochub.mongodb.org/core/resyncingaverystalereplicasetmember";

        // Activate maintenance mode and transition to RECOVERING.
        auto status = _replCoord->setMaintenanceMode(true);
        if (!status.isOK()) {
            warning() << "Failed to transition into maintenance mode: " << status;
        }
        status = _replCoord->setFollowerMode(MemberState::RS_RECOVERING);
        if (!status.isOK()) {
            warning() << "Failed to transition into " << MemberState(MemberState::RS_RECOVERING)
                      << ". Current state: " << _replCoord->getMemberState() << causedBy(status);
        }
        return;
    } else if (syncSourceResp.isOK() && !syncSourceResp.getSyncSource().empty()) {
        {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            _syncSourceHost = syncSourceResp.getSyncSource();
            source = _syncSourceHost;
        }
        // If our sync source has not changed, it is likely caused by our heartbeat data map being
        // out of date. In that case we sleep for 1 second to reduce the amount we spin waiting
        // for our map to update.
        if (oldSource == source) {
            log() << "Chose same sync source candidate as last time, " << source
                  << ". Sleeping for 1 second to avoid immediately choosing a new sync source for "
                     "the same reason as last time.";
            sleepsecs(1);
        } else {
            log() << "Changed sync source from "
                  << (oldSource.empty() ? std::string("empty") : oldSource.toString()) << " to "
                  << source;
        }
    } else {
        if (!syncSourceResp.isOK()) {
            log() << "failed to find sync source, received error "
                  << syncSourceResp.syncSourceStatus.getStatus();
        }
        // No sync source found.
        sleepsecs(1);
        return;
    }

    // If we find a good sync source after having gone too stale, disable maintenance mode so we can
    // transition to SECONDARY.
    if (_tooStale) {

        _tooStale = false;

        log() << "No longer too stale. Able to sync from " << source;

        auto status = _replCoord->setMaintenanceMode(false);
        if (!status.isOK()) {
            warning() << "Failed to leave maintenance mode: " << status;
        }
    }

    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        if (_state != ProducerState::Running) {
            return;
        }
        lastOpTimeFetched = _lastOpTimeFetched;
    }

    if (!_replCoord->getMemberState().primary()) {
        _replCoord->signalUpstreamUpdater();
    }

    // Set the applied point if unset. This is most likely the first time we've established a sync
    // source since stepping down or otherwise clearing the applied point. We need to set this here,
    // before the OplogWriter gets a chance to append to the oplog.
    {
        auto opCtx = cc().makeOperationContext();
        if (_replicationProcess->getConsistencyMarkers()->getAppliedThrough(opCtx.get()).isNull()) {
            _replicationProcess->getConsistencyMarkers()->setAppliedThrough(
                opCtx.get(), _replCoord->getMyLastAppliedOpTime());
        }
    }
    */

    mongo::sleepsecs(1);
    source = _syncSourceHost;
    log() << "MultiMaster _produce 1 starting GlobalFetcher with syncSource " << source;
    auto firstDocPolicy =
        (_lastOpTimeFetched != OpTime() ? GlobalFetcher::StartingPoint::kSkipFirstDoc
                                        : GlobalFetcher::StartingPoint::kEnqueueFirstDoc);
    // "lastFetched" not used. Already set in _enqueueDocuments.
    Status fetcherReturnStatus = Status::OK();
    DataReplicatorExternalStateGlobalSync dataReplicatorExternalState(
        _replCoord, _replicationCoordinatorExternalState, this);
    GlobalFetcher* oplogFetcher;
    try {
        auto onOplogFetcherShutdownCallbackFn = [&fetcherReturnStatus](const Status& status) {
            fetcherReturnStatus = status;
        };
        // The construction of GlobalFetcher has to be outside globalSync mutex, because it calls
        // replication coordinator.

        NamespaceString localOplogNS = NamespaceString("local.oplog.rs");
        NamespaceString remoteOplogNS = NamespaceString("local.oplog.rs");
        auto oplogFetcherPtr = stdx::make_unique<GlobalFetcher>(
            _replicationCoordinatorExternalState->getTaskExecutor(),
            _lastOpTimeFetched,
            source,
            remoteOplogNS,
            _replicationCoordinatorExternalState->getOplogFetcherSteadyStateMaxFetcherRestarts(),
            // syncSourceResp.rbid,
            0,
            false /* requireFresherSyncSource */,
            &dataReplicatorExternalState,
            [this](const auto& a1, const auto& a2, const auto& a3) {
                return this->_enqueueDocuments(a1, a2, a3);
            },
            onOplogFetcherShutdownCallbackFn,
            defaultBatchSize,
            _isMongodWithGlobalSync,
            _isMongoG,
            firstDocPolicy);
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        if (_state != ProducerState::Running) {
            log() << "MultiMaster producer state is not running";
            return;
        }
        _oplogFetcher = std::move(oplogFetcherPtr);
        oplogFetcher = _oplogFetcher.get();
    } catch (const mongo::DBException&) {
        fassertFailedWithStatus(51092, exceptionToStatus());
    }

    log() << "MultiMaster _produce 2 enqueFirstDoc: "
          << (firstDocPolicy == GlobalFetcher::StartingPoint::kEnqueueFirstDoc)
          << " find query filter: " << oplogFetcher->getFindQuery_forTest()["filter"];
    auto scheduleStatus = oplogFetcher->startup();
    if (!scheduleStatus.isOK()) {
        warning() << "unable to schedule fetcher to read remote oplog on " << source << ": "
                  << scheduleStatus;
        return;
    }
    log() << "MultiMaster _produce 3 status OK";

    oplogFetcher->join();
    log() << "fetcher stopped reading remote oplog on " << source;

    /*
    // If the background sync is stopped after the fetcher is started, we need to
    // re-evaluate our sync source and oplog common point.
    if (getState() != ProducerState::Running) {
        log() << "Replication producer stopped after oplog fetcher finished returning a batch from "
                 "our sync source.  Abandoning this batch of oplog entries and re-evaluating our "
                 "sync source.";
        return;
    }

    if (fetcherReturnStatus.code() == ErrorCodes::OplogOutOfOrder) {
        // This is bad because it means that our source
        // has not returned oplog entries in ascending ts order, and they need to be.

        warning() << redact(fetcherReturnStatus);
        // Do not blacklist the server here, it will be blacklisted when we try to reuse it,
        // if it can't return a matching oplog start from the last fetch oplog ts field.
        return;
    } else if (fetcherReturnStatus.code() == ErrorCodes::OplogStartMissing) {
        auto opCtx = cc().makeOperationContext();
        auto storageInterface = StorageInterface::get(opCtx.get());
        _runRollback(
            opCtx.get(), fetcherReturnStatus, source, syncSourceResp.rbid, storageInterface);
    } else if (fetcherReturnStatus == ErrorCodes::InvalidBSON) {
        Seconds blacklistDuration(60);
        warning() << "Fetcher got invalid BSON while querying oplog. Blacklisting sync source "
                  << source << " for " << blacklistDuration << ".";
        _replCoord->blacklistSyncSource(source, Date_t::now() + blacklistDuration);
    } else if (!fetcherReturnStatus.isOK()) {
        warning() << "Fetcher stopped querying remote oplog with error: "
                  << redact(fetcherReturnStatus);
    }
    */
}

Status GlobalSync::_enqueueDocuments(Fetcher::Documents::const_iterator begin,
                                     Fetcher::Documents::const_iterator end,
                                     const GlobalFetcher::DocumentsInfo& info) {
    // If this is the first batch of operations returned from the query, "toApplyDocumentCount" will
    // be one fewer than "networkDocumentCount" because the first document (which was applied
    // previously) is skipped.
    // std::string instanceId = computeInstanceId();

    log() << "MultiMaster _enqueueDocuments 1";
    if (info.toApplyDocumentCount == 0) {
        log() << "MultiMaster _enqueueDocuments 2 no documents to apply";
        return Status::OK();  // Nothing to do.
    }

    auto opCtx = cc().makeOperationContext();
    // POC: oplog.global must be capped!
    NamespaceString nss("local.oplog_global");
    // insert documents to the local.oplog_global
    DBDirectClient client(opCtx.get());

    if (_isMongoG) {
        invariant(!_isMongodWithGlobalSync);
        std::vector<BSONObj> docs;

        for (auto iDoc = begin; iDoc != end; ++iDoc) {
            if (iDoc->hasField("_gid")) {
                log() << "MultiMaster mongoG somehow got the _gid field in the doc already. "
                         "skipping: "
                      << *iDoc;
                continue;
            }
            log() << "MultiMaster mongoG _enqueueDocuments adding doc " << *iDoc;
            docs.emplace_back([&] {
                BSONObjBuilder newDoc;
                newDoc.append("_gid", _instanceId);
                newDoc.append("_lastMod",
                              LogicalClock::get(opCtx.get())->getClusterTime().asTimestamp());
                newDoc.appendElements(*iDoc);
                return newDoc.obj();
            }());
        }

        BatchedCommandRequest request([&] {
            write_ops::Insert insertOp(nss);
            insertOp.setDocuments(docs);
            return insertOp;
        }());

        const BSONObj cmd = request.toBSON();
        BSONObj res;

        log() << "MultiMaster mongoG _enqueueDocuments Running command: " << cmd;
        if (!client.runCommand(nss.db().toString(), cmd, res)) {
            log() << "MultiMaster mongoG _enqueueDocuments error running command: " << res;
            return getStatusFromCommandResult(res);
        }
        log() << "MultiMaster _enqueueDocuments Running command: OK";
    } else {
        invariant(_isMongodWithGlobalSync);
        BSONObjBuilder applyOpsBuilder;
        BSONArrayBuilder opsArray(applyOpsBuilder.subarrayStart("applyOps"_sd));
        for (auto iDoc = begin; iDoc != end; ++iDoc) {
            log() << "MultiMaster monogD _enqueueDocuments adding doc " << *iDoc;
            opsArray.append(*iDoc);
        }
        opsArray.done();
        applyOpsBuilder.append("allowAtomic", false);

        const auto cmd = applyOpsBuilder.done();
        const NamespaceString cmdNss{"admin", "$cmd"};
        BSONObj res;

        log() << "MultiMaster mongoD _enqueueDocuments Running command: " << cmd;
        if (!client.runCommand(cmdNss.db().toString(), cmd, res)) {
            log() << "MultiMaster mongoD _enqueueDocuments error running command: " << res;
            return getStatusFromCommandResult(res);
        }
        log() << "MultiMaster _enqueueDocuments Running command: OK";
    }

    {
        // Don't add more to the buffer if we are in shutdown. Continue holding the lock until we
        // are done to prevent going into shutdown. This avoids a race where shutdown() clears the
        // buffer between the time we check _inShutdown and the point where we finish writing to the
        // buffer.
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        if (_state != ProducerState::Running) {
            return Status::OK();
        }

        // Update last fetched info.
        _lastOpTimeFetched = info.lastDocument;
        LOG(3) << "batch resetting _lastOpTimeFetched: " << _lastOpTimeFetched;
    }

    // Check some things periodically (whenever we run out of items in the current cursor batch).
    if (info.networkDocumentBytes > 0 && info.networkDocumentBytes < kSmallBatchLimitBytes) {
        // On a very low latency network, if we don't wait a little, we'll be
        // getting ops to write almost one at a time.  This will both be expensive
        // for the upstream server as well as potentially defeating our parallel
        // application of batches on the secondary.
        //
        // The inference here is basically if the batch is really small, we are "caught up".
        sleepmillis(kSleepToAllowBatchingMillis);
    }

    log() << "MultiMaster _enqueueDocuments Finished OK";
    return Status::OK();
}

void GlobalSync::_runRollback(OperationContext* opCtx,
                              const Status& fetcherReturnStatus,
                              const HostAndPort& source,
                              int requiredRBID,
                              StorageInterface* storageInterface) {
    if (_replCoord->getMemberState().primary()) {
        warning() << "Rollback situation detected in catch-up mode. Aborting catch-up mode.";
        _replCoord->abortCatchupIfNeeded().transitional_ignore();
        return;
    }

    ShouldNotConflictWithSecondaryBatchApplicationBlock noConflict(opCtx->lockState());

    // Explicitly start future read transactions without a timestamp.
    opCtx->recoveryUnit()->setTimestampReadSource(RecoveryUnit::ReadSource::kNoTimestamp);

    // Rollback is a synchronous operation that uses the task executor and may not be
    // executed inside the fetcher callback.

    OpTime lastOpTimeFetched;
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        lastOpTimeFetched = _lastOpTimeFetched;
    }

    log() << "Starting rollback due to " << redact(fetcherReturnStatus);
    log() << "Replication commit point: " << _replCoord->getLastCommittedOpTime();

    // TODO: change this to call into the Applier directly to block until the applier is
    // drained.
    //
    // Wait till all buffered oplog entries have drained and been applied.
    auto lastApplied = _replCoord->getMyLastAppliedOpTime();
    if (lastApplied != lastOpTimeFetched) {
        log() << "Waiting for all operations from " << lastApplied << " until " << lastOpTimeFetched
              << " to be applied before starting rollback.";
        while (lastOpTimeFetched > (lastApplied = _replCoord->getMyLastAppliedOpTime())) {
            sleepmillis(10);
            if (getState() != ProducerState::Running) {
                return;
            }
        }
    }

    OplogInterfaceLocal localOplog(opCtx, NamespaceString::kRsOplogNamespace.ns());

    const int messagingPortTags = 0;
    ConnectionPool connectionPool(messagingPortTags);
    std::unique_ptr<ConnectionPool::ConnectionPtr> connection;
    auto getConnection = [&connection, &connectionPool, source]() -> DBClientBase* {
        if (!connection.get()) {
            connection.reset(new ConnectionPool::ConnectionPtr(
                &connectionPool, source, Date_t::now(), kRollbackOplogSocketTimeout));
        };
        return connection->get();
    };

    // Because oplog visibility is updated asynchronously, wait until all uncommitted oplog entries
    // are visible before potentially truncating the oplog.
    storageInterface->waitForAllEarlierOplogWritesToBeVisible(opCtx);

    auto storageEngine = opCtx->getServiceContext()->getStorageEngine();
    if (!forceRollbackViaRefetchByDefault && storageEngine->supportsRecoverToStableTimestamp()) {
        log() << "Rollback using 'recoverToStableTimestamp' method.";
        _runRollbackViaRecoverToCheckpoint(
            opCtx, source, &localOplog, storageInterface, getConnection);
    } else {
        log() << "Rollback using the 'rollbackViaRefetch' method.";
        _fallBackOnRollbackViaRefetch(opCtx, source, requiredRBID, &localOplog, getConnection);
    }

    // Reset the producer to clear the sync source and the last optime fetched.
    stop(true);
    startProducerIfStopped();
}

void GlobalSync::_runRollbackViaRecoverToCheckpoint(
    OperationContext* opCtx,
    const HostAndPort& source,
    OplogInterface* localOplog,
    StorageInterface* storageInterface,
    OplogInterfaceRemote::GetConnectionFn getConnection) {

    OplogInterfaceRemote remoteOplog(
        source, getConnection, NamespaceString::kRsOplogNamespace.ns(), defaultRollbackBatchSize);

    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        if (_state != ProducerState::Running) {
            return;
        }
    }

    log() << "MultiMaster: skipping rollback, not implemented";
    /*
    _rollback = stdx::make_unique<RollbackImpl>(
        localOplog, &remoteOplog, storageInterface, _replicationProcess, _replCoord);

    log() << "Scheduling rollback (sync source: " << source << ")";
    auto status = _rollback->runRollback(opCtx);
    if (status.isOK()) {
        log() << "Rollback successful.";
    } else if (status == ErrorCodes::UnrecoverableRollbackError) {
        severe() << "Rollback failed with unrecoverable error: " << status;
        fassertFailedWithStatusNoTrace(51093, status);
    } else {
        warning() << "Rollback failed with retryable error: " << status;
    }
    */
}

void GlobalSync::_fallBackOnRollbackViaRefetch(
    OperationContext* opCtx,
    const HostAndPort& source,
    int requiredRBID,
    OplogInterface* localOplog,
    OplogInterfaceRemote::GetConnectionFn getConnection) {

    RollbackSourceImpl rollbackSource(
        getConnection, source, NamespaceString::kRsOplogNamespace.ns(), defaultRollbackBatchSize);

    log() << "MultiMaster: skipping rollback, not implemented";
    //  rollback(opCtx, *localOplog, rollbackSource, requiredRBID, _replCoord, _replicationProcess);
}

HostAndPort GlobalSync::getSyncTarget() const {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    return _syncSourceHost;
}

void GlobalSync::clearSyncTarget() {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    log() << "Resetting sync source to empty, which was " << _syncSourceHost;
    _syncSourceHost = HostAndPort();
}

void GlobalSync::stop(bool resetLastFetchedOptime) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    _state = ProducerState::Stopped;
    log() << "Stopping replication producer";

    _syncSourceHost = HostAndPort();
    if (resetLastFetchedOptime) {
        _lastOpTimeFetched = OpTime();
        log() << "Resetting last fetched optimes in globalSync";
    }

    if (_syncSourceResolver) {
        _syncSourceResolver->shutdown();
    }

    if (_oplogFetcher) {
        _oplogFetcher->shutdown();
    }
}

void GlobalSync::start(OperationContext* opCtx) {
    OpTime lastAppliedOpTime;
    ShouldNotConflictWithSecondaryBatchApplicationBlock noConflict(opCtx->lockState());

    // Explicitly start future read transactions without a timestamp.
    opCtx->recoveryUnit()->setTimestampReadSource(RecoveryUnit::ReadSource::kNoTimestamp);

    /*
    do {
        lastAppliedOpTime = _readLastAppliedOpTime(opCtx);
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        // Double check the state after acquiring the mutex.
        if (_state != ProducerState::Starting) {
            return;
        }
        _state = ProducerState::Running;

        // When a node steps down during drain mode, the last fetched optime would be newer than
        // the last applied.
        if (_lastOpTimeFetched <= lastAppliedOpTime) {
            log() << "MUltiMaster Setting globalSync _lastOpTimeFetched=" << lastAppliedOpTime
                  << ". Previous _lastOpTimeFetched: " << _lastOpTimeFetched;
            _lastOpTimeFetched = lastAppliedOpTime;
        }
        // Reload the last applied optime from disk if it has been changed.
    } while (lastAppliedOpTime != _replCoord->getMyLastAppliedOpTime());
    */
    if (_state != ProducerState::Starting) {
        return;
    }
    _state = ProducerState::Running;


    log() << "MultiMaster starting globalSync, fetch queue set to: " << _lastOpTimeFetched
          << " source: " << _syncSourceHost;
}

// TODO read only entry from synced collections
OpTime GlobalSync::_readLastAppliedOpTime(OperationContext* opCtx) {
    BSONObj oplogEntry;
    StringData mmCollName = "mm_replication.MultiMasterCollection";
    try {
        bool success = writeConflictRetry(
            opCtx, "readLastAppliedOpTime", NamespaceString::kRsOplogNamespace.ns(), [&] {
                Lock::DBLock lk(opCtx, "local", MODE_X);
                // return Helpers::findOne(opCtx, NamespaceString::kRsOplogNamespace.ns().c_str(),
                //    BSON( "ns" << mmCollName) ,oplogEntry);
                return Helpers::getFirst(  // POC - using just some time in the past, needs to
                                           // switch to global sync
                    opCtx,
                    NamespaceString::kRsOplogNamespace.ns().c_str(),
                    oplogEntry);
            });

        if (!success) {
            log() << "Error reading last entry of oplog in globalSync";
            // This can happen when we are to do an initial sync.
            return OpTime();
        }
    } catch (const ExceptionForCat<ErrorCategory::ShutdownError>&) {
        throw;
    } catch (const DBException& ex) {
        severe() << "Problem reading " << NamespaceString::kRsOplogNamespace.ns() << ": "
                 << redact(ex);
        fassertFailed(51091);
    }

    OplogEntry parsedEntry(oplogEntry);
    log() << "Successfully read last entry of oplog while starting globalSync: "
          << redact(oplogEntry);
    return parsedEntry.getOpTime();
}

bool GlobalSync::shouldStopFetching() const {
    // Check if we have been stopped.
    if (getState() != ProducerState::Running) {
        LOG(2) << "Stopping oplog fetcher due to stop request.";
        return true;
    }

    // Check current sync source.
    if (getSyncTarget().empty()) {
        LOG(1) << "Stopping oplog fetcher; canceling oplog query because we have no valid sync "
                  "source.";
        return true;
    }

    return false;
}

GlobalSync::ProducerState GlobalSync::getState() const {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _state;
}

void GlobalSync::startProducerIfStopped() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    // Let producer run if it's already running.
    if (_state == ProducerState::Stopped) {
        _state = ProducerState::Starting;
    }
}


}  // namespace repl
}  // namespace mongo
