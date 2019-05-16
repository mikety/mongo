/**************||||****************
 *                                *
 *  Here could be your copyright  *
 *                                *
 ***************||*****************
                ||
                ||            \
                || == CosmosDB \
  /             || == 1 year   /
 / DynamoDB === ||            /
 \   4years === ||
  \             ||
                ||
                ||
                ||
 \\\\//// \\\//2019\\ //\//////// */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplicationInitialSync

#include "mongo/platform/basic.h"

#include "mongo/db/mm/multi_syncer.h"

#include <algorithm>
#include <utility>

#include "mongo/base/counter.h"
#include "mongo/base/status.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/fetcher.h"
#include "mongo/client/remote_command_retry_scheduler.h"
#include "mongo/db/commands/feature_compatibility_version_parser.h"
#include "mongo/db/commands/server_status_metric.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/logical_clock.h"
#include "mongo/db/mm/global_fetcher.h"
#include "mongo/db/mm/global_sync.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/databases_cloner.h"
#include "mongo/db/repl/initial_sync_state.h"
#include "mongo/db/repl/member_state.h"
#include "mongo/db/repl/oplog_buffer.h"
#include "mongo/db/repl/oplog_fetcher.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/replication_consistency_markers.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/sync_source_selector.h"
#include "mongo/db/server_parameters.h"
#include "mongo/executor/task_executor.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/rpc/metadata/repl_set_metadata.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/destructor_guard.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"
#include "mongo/util/timer.h"

namespace mongo {

MONGO_EXPORT_SERVER_PARAMETER(isMongodWithGlobalSync, bool, false);
MONGO_EXPORT_SERVER_PARAMETER(isMongoG, bool, false);

MONGO_EXPORT_SERVER_PARAMETER(mmPortConfig, int, 20017);
MONGO_EXPORT_SERVER_PARAMETER(mmPort1, int, 20017);
MONGO_EXPORT_SERVER_PARAMETER(mmPort2, int, 20017);
MONGO_EXPORT_SERVER_PARAMETER(mmPort3, int, 20017);

namespace repl {

namespace {
using namespace executor;
using QueryResponseStatus = StatusWith<Fetcher::QueryResponse>;
using UniqueLock = stdx::unique_lock<stdx::mutex>;
using LockGuard = stdx::lock_guard<stdx::mutex>;


ServiceContext::UniqueOperationContext makeOpCtx() {
    return cc().makeOperationContext();
}

StatusWith<Timestamp> parseTimestampStatus(const QueryResponseStatus& fetchResult) {
    if (!fetchResult.isOK()) {
        return fetchResult.getStatus();
    } else {
        const auto docs = fetchResult.getValue().documents;
        const auto hasDoc = docs.begin() != docs.end();
        if (!hasDoc || !docs.begin()->hasField("ts")) {
            return {ErrorCodes::FailedToParse, "Could not find an oplog entry with 'ts' field."};
        } else {
            return {docs.begin()->getField("ts").timestamp()};
        }
    }
}

StatusWith<OpTime> parseOpTime(const QueryResponseStatus& fetchResult) {
    if (!fetchResult.isOK()) {
        return fetchResult.getStatus();
    }
    const auto docs = fetchResult.getValue().documents;
    const auto hasDoc = docs.begin() != docs.end();
    return hasDoc ? OpTime::parseFromOplogEntry(docs.front())
                  : StatusWith<OpTime>{ErrorCodes::NoMatchingDocument, "no oplog entry found"};
}

std::string computeInstanceId(int syncSourcePort) {
    std::string hostName = getHostNameCached();
    return str::stream() << hostName << ":" << syncSourcePort;
}

}  // namespace

/* MultiSyncer */

MultiSyncer::MultiSyncer(//GlobalInitialSyncerOptions opts,
                         std::unique_ptr<DataReplicatorExternalState> dataReplicatorExternalState,
                         ReplicationCoordinator* replicationCoordinator,
                         ReplicationCoordinatorExternalState* replicationCoordinatorExternalState,
                         OnCompletionFn onCompletion)
    : _dataReplicatorExternalState(std::move(dataReplicatorExternalState)),
      _replicationCoordinator(replicationCoordinator),
      _replicationCoordinatorExternalState(replicationCoordinatorExternalState),
      _onCompletion(onCompletion) {

    if (isMongodWithGlobalSync.load()) {
        auto syncSourcePort = mmPortConfig.load();
        auto syncSource = HostAndPort("localhost", syncSourcePort);
        auto instanceId = computeInstanceId(syncSourcePort);  // TODO: change to its own local port

        auto syncer = std::make_unique<GlobalSync>(
            _replicationCoordinator, 
            _replicationCoordinatorExternalState,
             syncSource,
             instanceId,
             true,
             false,
             OpTime());
        /*
        GlobalInitialSyncerOptions options(opts);
        auto syncer = stdx::make_unique<GlobalInitialSyncer>(
            options,
            _dataReplicatorExternalState.get(),
            syncSource,
            instanceId,
            true,
            false,
            [=](StatusWith<OpTime> lastFetched) {
                return initialSyncCompleted(syncSource, instanceId, lastFetched);
            });
        _initialSyncers.emplace_back(std::move(syncer));
        */
        _syncers.emplace_back(std::move(syncer));
        log() << "MultiMaster Computed CurrentSyncSource for local syncer global oplog: "
              << syncSourcePort;
    }
    if (isMongoG.load()) {
        std::vector<int> ports;
        ports.push_back(mmPort1.load());
        ports.push_back(mmPort2.load());
        ports.push_back(mmPort3.load());

        for (const auto syncSourcePort : ports) {
            auto syncSource = HostAndPort("localhost", syncSourcePort);
            auto instanceId = computeInstanceId(syncSourcePort);
            auto syncer = std::make_unique<GlobalSync>(
                _replicationCoordinator, 
                _replicationCoordinatorExternalState,
                 syncSource,
                 instanceId,
                false,
                true,
                 OpTime());
            /*
            GlobalInitialSyncerOptions options(opts);

            auto syncer = stdx::make_unique<GlobalInitialSyncer>(
                options,
                _dataReplicatorExternalState.get(),
                syncSource,
                instanceId,
                false,
                true,
                [=](StatusWith<OpTime> lastFetched) {
                    return initialSyncCompleted(syncSource, instanceId, lastFetched);
                });
            _initialSyncers.emplace_back(std::move(syncer));
            */
            _syncers.emplace_back(std::move(syncer));
            log() << "MultiMaster Computed CurrentSyncSource for MongoG for host: "
                  << syncSourcePort;
        }
    }
}

MultiSyncer::~MultiSyncer() {
    log() << "MultiMaster ~MultiSyncer";
}

/*
Status MultiSyncer::initialSyncCompleted(HostAndPort syncSource,
                                         std::string instanceId,
                                         StatusWith<OpTime> lastFetched) {
    log() << "MultiMaster Initial Syncer for: " << syncSource << " completed";
    _activeSyncCount--;
    auto syncer = std::make_unique<GlobalSync>(_replicationCoordinator,
                                               _replicationCoordinatorExternalState,
                                               syncSource,
                                               instanceId,
                                               lastFetched.getValue());
    _steadySyncers.emplace_back(std::move(syncer));
    auto res = Status::OK();
    if (_initialSyncActive && _activeSyncCount == 0) {
        _initialSyncActive = false;
        res = _startSteadyReplication();
    }
    return res;
}
*/


Status MultiSyncer::startup(std::uint32_t maxAttempts) noexcept {
    auto res = Status::OK();
    for (const auto& sync : _syncers) {
        log() << "MultiMaster starting initial sync for syncer on instance: "
              << sync->getInstanceId();
        sync->startup();
        _activeSyncCount++;
    }
    _syncActive = true;
    return res;
}

bool MultiSyncer::isActive() const {
    return true;
}

// TODO futures array will be perfect here
void MultiSyncer::join() {
    for (const auto& sync : _syncers) {
        log() << "MultiMaster joining steady sync on instance: " << sync->getInstanceId();
        sync->join();
    }
}

// TODO futures array will be perfect here
Status MultiSyncer::shutdown() {
    auto res = Status::OK();
    for (const auto& sync : _syncers) {
        log() << "MultiMaster shutting down steady sync on instance: " << sync->getInstanceId();
        sync->shutdown();
    }
    return res;
}

}  // namespace repl
}  // namespace mongo
