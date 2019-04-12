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

#include "mongo/db/operation_context.h"


namespace mongo {

class GlobalApplyTracker {
public:
    static const OperationContext::Decoration<GlobalApplyTracker> get;

    GlobalApplyTracker() = default;
    void setIsRemote(bool isRemote) { _isRemote = isRemote; }
    bool isRemote() const { return _isRemote; }
 private:
   bool _isRemote{false}; 
};

} // namespace mongo
