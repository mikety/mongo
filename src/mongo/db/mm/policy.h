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

#include "mongo/db/mm/vector_time.h"

namespace mongo {

class Policy {
public:
    static bool shouldUpdate(const GlobalEvent& l, const GlobalEvent& r);
    static bool isConflict(const GlobalEvent& l, const GlobalEvent& r);
};

}  // napespace mongo
