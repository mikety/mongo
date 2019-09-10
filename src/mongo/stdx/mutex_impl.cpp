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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kTrackLock

#include "mongo/platform/basic.h"
#include <sstream>

#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/log.h"

namespace mongo {

namespace {
std::unique_ptr<LockActions> gLockActions;
}

namespace stdx {

mutex::mutex(const StringData& file, unsigned int line): _isTraced(true), _name("DebugMutex"_sd)
{
  _mutexId = file + ":" + std::to_string(line);
}

mutex::mutex(const StringData& name) : _name(name) {}

void mutex::lock() {
    if (_isTraced) {
        std::stringstream l;
        l << "#LOCK_MUTEX#" << stdx::this_thread::get_id() << "#" << _mutexId << "#UNDEF"; 
        log() << l.str();
    }
    auto hasLock = _mutex.try_lock_for(kContendedLockTimeout.toSystemDuration());
    if (hasLock) {
        return;
    }
    hasLock = _mutex.try_lock_for(_lockTimeout.toSystemDuration() -
                                  kContendedLockTimeout.toSystemDuration());
    uassert(
        ErrorCodes::InternalError, "Unable to take latch, wait time exceeds set timeout", hasLock);
}

void mutex::unlock() {
    if (_isTraced) {
        std::stringstream l;
        l << "#UNLOCK_MUTEX#" << stdx::this_thread::get_id() << "#" << _mutexId << "#UNDEF"; 
        log() << l.str();
    }
    _mutex.unlock();
}

void mutex::lock(const std::string& text) {
    if (_isTraced) {
        std::stringstream l;
        l << "#LOCK_DETAILS#" << stdx::this_thread::get_id() << "#" << _mutexId << "#" << text; 
        log() << l.str();
    }
    auto hasLock = _mutex.try_lock_for(kContendedLockTimeout.toSystemDuration());
    if (hasLock) {
        return;
    }
    hasLock = _mutex.try_lock_for(_lockTimeout.toSystemDuration() -
                                  kContendedLockTimeout.toSystemDuration());
    uassert(
        ErrorCodes::InternalError, "Unable to take latch, wait time exceeds set timeout", hasLock);
}

void mutex::unlock(const std::string& text) {
    if (_isTraced) {
        std::stringstream l;
        l << "#UNLOCK_DETAILS#" << stdx::this_thread::get_id() << "#" << _mutexId << "#" << text; 
        log() << l.str();
    }
    _mutex.unlock();
}

bool mutex::try_lock() {
    return _mutex.try_lock();
}

void mutex::setLockActions(std::unique_ptr<LockActions> actions) {
    gLockActions = std::move(actions);
}

TracedLockGuard::TracedLockGuard(mutex& m, const StringData& file, unsigned int line):
    _lock(m, std::adopt_lock_t())
{
    _lockId = file + ":" + std::to_string(line);
    std::stringstream l;
    l << "#LOCK_GUARD#" << stdx::this_thread::get_id() << "#" << "UNDEF" << "#" << _lockId; 
    log() << l.str();
    m.lock(_lockId);
}

TracedLockGuard::~TracedLockGuard() {
    std::stringstream l;
    l << "#UNLOCK_GUARD#" << stdx::this_thread::get_id() << "#" << "UNDEF" << "#" << _lockId; 
    log() << l.str();
}

}  // namespace stdx
}  // namespace mongo
