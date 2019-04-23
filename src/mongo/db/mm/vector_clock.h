
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

#include <map>

#include "mongo/db/mm/vector_time.h"

namespace mongo {
class ServiceContext;
class OperationContext;

class VectorClock {
public:
    static VectorClock* get(ServiceContext* service);
    static VectorClock* get(OperationContext* ctx);
    static void set(ServiceContext* service, std::unique_ptr<VectorClock> vectorClock);

    VectorClock(ServiceContext* service, size_t nodeId);

    // reserves ticks on the current node
    VectorTime reserveTicks(uint64_t nTicks);

    VectorTime getGlobalTime();

    Status advanceGlobalTime(const VectorTime newTime);

    void setGlobalTime(VectorTime newTime);

    void syncClusterTime();
private:
    mutable stdx::mutex _mutex;

    ServiceContext* const _service;

    std::map<int, std::string> _nodes;
    size_t _nodeId;
    VectorTime _globalTime;
};

} // namespace mongo
