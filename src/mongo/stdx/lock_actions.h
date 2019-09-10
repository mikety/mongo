/* Impl */

#pragma once

#include <string>
#include "mongo/stdx/mutex.h"

namespace mongo {

class LockActionsImpl: public LockActions {
public:
    virtual void onLockTraced(const std::string&) override;
    virtual void onUnlockTraced(const std::string&) override;
};

} /// namespace mongo
