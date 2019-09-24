/* Impl */

#pragma once

#include "mongo/stdx/mutex.h"
#include <string>

namespace mongo {

class LockActionsImpl : public LockActions {
public:
    virtual void onLockTraced(const std::string&) override;
    virtual void onUnlockTraced(const std::string&) override;
};

}  // namespace mongo
