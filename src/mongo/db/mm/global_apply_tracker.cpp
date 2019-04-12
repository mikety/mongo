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

#include "mongo/db/mm/global_apply_tracker.h"

namespace mongo {

const OperationContext::Decoration<GlobalApplyTracker> GlobalApplyTracker::get = 
    OperationContext::declareDecoration<GlobalApplyTracker>();

} // namespace mongo
