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

#include <vector>

#include "mongo/bson/timestamp.h"
#include "mongo/db/logical_time.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"

namespace mongo {

class VectorTime {
public:
    static const size_t NodeVectorSize{3};

    VectorTime();

    std::string toString() const; 

    void addTicksToNode(size_t nodeId, uint32_t nTicks);
    void setTimeForNode(size_t nodeId, LogicalTime lTime);
    
    void advance(const VectorTime& newTime);

    LogicalTime timeAtNode(size_t nodeId) const {
        return _time[nodeId];
    }
    
    static VectorTime fromBSON(BSONElement bson);

    void appendAsBSON(BSONObjBuilder* builder) const;
private:
    // position based vector time
    std::vector<LogicalTime> _time;
};


// base for all events to compare
// global event contains when, and where and what is in the descendant class
class GlobalEvent {
public:
    GlobalEvent(VectorTime time, size_t nodeId);

    // the argument has happened before the caller on the argument's node.
    virtual bool happenedBefore(const GlobalEvent& r) const;

    virtual ~GlobalEvent() {}

private:
    VectorTime _globalTime;
    size_t _nodeId;
};

inline std::ostream& operator<<(std::ostream& s, const VectorTime& v) {
    return (s << v.toString());
}

} // namespace mongo
