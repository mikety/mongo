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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/db/mm/policy.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

/**
 * POC function with hardcoded nodes
 * node1 hasPriority over node2
 */
bool hasPriority(size_t node1, size_t node2) {
    size_t n1(0);  // highest priority
    size_t n2(1);
    size_t n3(2);

    invariant(node1 == n1 || node1 == n2 || node1 == n3);
    invariant(node2 == n1 || node2 == n2 || node2 == n3);

    if (node1 == node2) {
        return false;
    }

    if (node1 == n1 || node2 == n3) {
        return true;
    }

    return false;
}

}  // namespace

bool Policy::isConflict(const GlobalEvent& before, const GlobalEvent& after) {
    return !before.hb(after) && !after.hb(before);
}

bool Policy::shouldUpdate(const GlobalEvent& oldEvent, const GlobalEvent& newEvent) {
    // check if events are happened before
    if (oldEvent.hb(newEvent)) {
        return true;  // keep the new event
    }

    if (newEvent.hb(oldEvent)) {
        return false;  // keep the old event
    }

    // conflict
    // use node priority but it should be policy specific
    bool shouldReplace = !hasPriority(oldEvent.nodeId(), newEvent.nodeId());
    log() << "MultiMaster Policy::shouldUpdate found conflict: oldEvent: " << oldEvent
          << " newEvent: " << newEvent << " result: " << shouldReplace;
    return shouldReplace;
}
}  // namespace mongo
