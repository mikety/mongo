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

#include "mongo/db/mm/policy.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"

namespace mongo {
namespace {

/**
 * POC function with hardcoded nodes
 * n3 < n2 < n1
 */
bool lessThan(size_t node1, size_t node2) {
    size_t  n1(0);
    size_t  n2(1);
    size_t  n3(2);

    invariant(node1 == n1 || node1 == n2 || node1 == n3);
    invariant(node2 == n1 || node2 == n2 || node2 == n3);

    if (node1 == node2) {
        return false;
    }

    if (node1 == n3 || node2 == n1) {
        return true;
    }

    return false;
}

}  // namespace

bool Policy::isConflict(const GlobalEvent& l, const GlobalEvent& r) {
    return !l.hb(r) && !r.hb(l);
}
    
bool Policy::shouldUpdate(const GlobalEvent& pastEvent, const GlobalEvent& curEvent) {
    // check if events are happened before
    if (pastEvent.hb(curEvent)) {
        return false; // keep the current event
    }

    if (curEvent.hb(pastEvent)) {
        return true; // override the current event
    }

    // conflict
    // use node priority but it should be policy specific
    return lessThan(curEvent.nodeId(), pastEvent.nodeId());
}
} // namespace mongo
