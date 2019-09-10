#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kTrackLock

#include "mongo/platform/basic.h"

#include <unordered_map>
#include <string>
#include <tuple>

#include "mongo/stdx/lock_actions.h"
#include "mongo/stdx/thread.h"

#include "mongo/util/log.h"

namespace mongo {

void LockActionsImpl::onLockTraced(const std::string& text) {
    log() << text;
} 

void LockActionsImpl::onUnlockTraced(const std::string& text) {
    log() << text;
} 

} // namespace mongo
