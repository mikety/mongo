
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/db/mm/vector_clock.h"

#include "mongo/base/status.h"
#include "mongo/db/global_settings.h"
#include "mongo/db/logical_clock.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/service_context.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {
const auto getVectorClock = ServiceContext::declareDecoration<std::unique_ptr<VectorClock>>();
}

VectorClock* VectorClock::get(ServiceContext* service) {
    return getVectorClock(service).get();
}

VectorClock* VectorClock::get(OperationContext* ctx) {
    return get(ctx->getClient()->getServiceContext());
}

void VectorClock::set(ServiceContext* service, std::unique_ptr<VectorClock> clockArg) {
    auto& clock = getVectorClock(service);
    clock = std::move(clockArg);
}

VectorClock::VectorClock( ServiceContext* service, size_t nodeId ) : 
    _service(service), _nodeId(nodeId) 
{}

VectorTime VectorClock::getGlobalTime() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _globalTime;
}

void VectorClock::setGlobalTime(VectorTime newTime) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _globalTime = newTime;
}

Status VectorClock::advanceGlobalTime(const VectorTime newTime) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _globalTime.advance(newTime);
    return Status::OK();
}

VectorTime VectorClock::reserveTicks(uint64_t nTicks) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _globalTime.addTicksToNode(_nodeId, nTicks);
    return _globalTime;
}

void VectorClock::syncClusterTime() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _globalTime.setTimeForNode(_nodeId, LogicalClock::get(_service)->getClusterTime());
}

} // namespace mongo
