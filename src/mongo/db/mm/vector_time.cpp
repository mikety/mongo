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

#include "mongo/platform/basic.h"

#include "mongo/db/logical_time.h"
#include "mongo/db/mm/vector_time.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"

namespace mongo {
namespace {

constexpr auto kOperationTime = "operationTime"_sd;

}  // namespace

VectorTime::VectorTime() : _time(NodeVectorSize) {}

void VectorTime::addTicksToNode(size_t nodeId, uint32_t nTicks) {
    invariant(nodeId <= NodeVectorSize);

    _time[nodeId].addTicks(nTicks);
}

void VectorTime::setTimeForNode(size_t nodeId, LogicalTime lTime) {
    invariant(nodeId <= NodeVectorSize);

    _time[nodeId] = lTime;
}

void VectorTime::advance(const VectorTime& newTime) {
    for (size_t i = 0; i < NodeVectorSize; ++i) {
        if (_time[i] < newTime._time[i]) {
            _time[i] = newTime._time[i];
        }
    }
}

VectorTime VectorTime::fromBSON(BSONElement bson) {
    auto newTime = bson.Array();
    VectorTime vt;
    decltype(newTime)::size_type pos = 0;
    for (pos = 0; pos < newTime.size(); ++pos) {
        auto nodeTime = newTime.at(pos).timestamp();
        vt._time[pos] = LogicalTime(nodeTime);
    }
    return vt;
}

void VectorTime::appendAsBSON(BSONObjBuilder* builder) const {

    BSONArrayBuilder arrBuilder;
    for (size_t i = 0; i < NodeVectorSize; ++i) {
        arrBuilder.append(_time[i].asTimestamp());
    }
    builder->append("_globalTs", arrBuilder.arr());
}

std::string VectorTime::toString() const {
    std::string res = "[";
    for (size_t i = 0; i < NodeVectorSize; ++i) {
        res += _time[i].toString();
        res += " ";
    }
    res += "]";
    return res;
}
// GlobalEvent

GlobalEvent::GlobalEvent(VectorTime time, size_t nodeId) : _globalTime(time), _nodeId(nodeId) {}

bool GlobalEvent::hb(const GlobalEvent& after) const {
    for (size_t nodeId = 0; nodeId < VectorTime::NodeVectorSize; ++nodeId) {
        if (_globalTime.timeAtNode(nodeId) > after._globalTime.timeAtNode(nodeId)) {
            return false;
        }
    }
    return true;
}

std::string GlobalEvent::toString() const {
    std::ostringstream res;
    res << "Node: " << _nodeId << " Event Time: " << _globalTime.toString();
    return res.str();
}

}  // namespace mongo
