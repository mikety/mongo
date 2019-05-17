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

#pragma once

#include <cstdint>
#include <iosfwd>
#include <memory>

#include "mongo/base/status.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/mm/global_fetcher.h"
#include "mongo/db/mm/global_sync.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/callback_completion_guard.h"
#include "mongo/db/repl/collection_cloner.h"
#include "mongo/db/repl/data_replicator_external_state.h"
#include "mongo/db/repl/database_cloner.h"
#include "mongo/db/repl/multiapplier.h"
#include "mongo/db/repl/oplog_applier.h"
#include "mongo/db/repl/oplog_buffer.h"
#include "mongo/db/repl/oplog_fetcher.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/rollback_checker.h"
#include "mongo/db/repl/sync_source_selector.h"
#include "mongo/dbtests/mock/mock_dbclient_connection.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/functional.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {
namespace repl {

class GlobalInitialSyncerOptions;
class GlobalInitialSyncer;
class GlobalSync;

class MultiSyncer {
    MONGO_DISALLOW_COPYING(MultiSyncer);

public:
    typedef stdx::function<void(const OpTime& lastApplied)> OnCompletionFn;

    MultiSyncer(  // const GlobalInitialSyncerOptions& opts,
        std::unique_ptr<DataReplicatorExternalState> dataReplicatorExternalState,
        ReplicationCoordinator* replicationCoordinator,
        ReplicationCoordinatorExternalState* replicationCoordinatorExternalState,
        OnCompletionFn onCompletion);

    ~MultiSyncer();

    /**
     * Starts initial sync process, with the provided number of attempts
     */
    Status startup(std::uint32_t maxAttempts) noexcept;

    /**
     * Returns an informational string.
     */
    std::string toString() const;

    /**
     * Returns true if a remote command has been scheduled (but not completed)
     * with the executor.
     */
    bool isActive() const;

    /**
     * Shuts down replication if "start" has been called, and blocks until shutdown has completed.
     */
    Status shutdown();

    /**
     * Waits for remote command requests to complete.
     * Returns immediately if fetcher is not active.
     */
    void join();

    /*
    Status initialSyncCompleted(HostAndPort syncSource,
                                std::string instanceId,
                                StatusWith<OpTime> lastFetched);
    */

private:
    Status _startSteadyReplication();

    std::vector<std::unique_ptr<GlobalSync>> _syncers;
    std::unique_ptr<DataReplicatorExternalState> _dataReplicatorExternalState;
    ReplicationCoordinator* _replicationCoordinator;
    ReplicationCoordinatorExternalState* _replicationCoordinatorExternalState;
    OnCompletionFn _onCompletion;
    int _activeSyncCount{0};
    bool _syncActive{false};
};

}  // namespace repl
}  // namespace mongo
