/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/util/duration.h"
#include <mutex>
#include <string>

namespace mongo {
class LockActions {
public:
    virtual ~LockActions() = default;
    virtual void onContendedLock(const StringData& name){};
    virtual void onLockTraced(const std::string& text){};
    virtual void onUnlockTraced(const std::string& text){};
    virtual void onUnlock(){};
};

namespace stdx {

using ::std::recursive_mutex;  // NOLINT
using ::std::timed_mutex;      // NOLINT

using ::std::adopt_lock_t;   // NOLINT
using ::std::defer_lock_t;   // NOLINT
using ::std::try_to_lock_t;  // NOLINT

using ::std::lock_guard;   // NOLINT
using ::std::unique_lock;  // NOLINT

constexpr adopt_lock_t adopt_lock{};
constexpr defer_lock_t defer_lock{};
constexpr try_to_lock_t try_to_lock{};

class mutex {
public:
    mutex() : mutex("AnonymousMutex"_sd) {}
    explicit mutex(const StringData& name);
    mutex(const StringData&, unsigned int);

    void lock();
    void lock(const std::string& text);
    void unlock();
    void unlock(const std::string& text);
    bool try_lock();
    const StringData& getName() {
        return _name;
    }

    static void setLockActions(std::unique_ptr<LockActions> actions);

private:
    bool _isTraced{false};
    std::string _mutexId;
    const StringData _name;
    const Seconds _lockTimeout = Seconds(60);
    static constexpr Milliseconds kContendedLockTimeout = Milliseconds(100);
    ::std::timed_mutex _mutex;
};

class TracedLockGuard {
public:
    TracedLockGuard(mutex& m, const StringData&, unsigned int);
    ~TracedLockGuard();

private:
    std::string _lockId;
    stdx::lock_guard<mutex> _lock;
};

}  // namespace stdx
}  // namespace mongo
