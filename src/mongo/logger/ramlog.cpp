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

#include "mongo/platform/basic.h"

#include "mongo/logger/ramlog.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/logger/message_event_utf8_encoder.h"
#include "mongo/util/map_util.h"
#include "mongo/util/str.h"

namespace mongo {

using std::string;

namespace {
typedef std::map<string, RamLog*> RM;
std::mutex* _namedLock = nullptr;
RM* _named = nullptr;

}  // namespace

RamLog::RamLog(const std::string& name) : _name(name), _totalLinesWritten(0), _lastWrite(0) {
    clear();
    for (int i = 0; i < N; i++)
        lines[i][C - 1] = 0;
}

RamLog::~RamLog() {}

void RamLog::write(const std::string& str) {
    std::lock_guard<std::mutex> lk(_mutex);
    _lastWrite = time(nullptr);
    _totalLinesWritten++;

    char* p = lines[(h + n) % N];

    unsigned sz = str.size();
    if (0 == sz)
        return;
    if (sz < C) {
        if (str.c_str()[sz - 1] == '\n') {
            memcpy(p, str.c_str(), sz - 1);
            p[sz - 1] = 0;
        } else
            memcpy(p, str.c_str(), sz);
    } else {
        memcpy(p, str.c_str(), C - 1);
    }

    if (n < N)
        n++;
    else
        h = (h + 1) % N;
}

void RamLog::clear() {
    std::lock_guard<std::mutex> lk(_mutex);
    _totalLinesWritten = 0;
    _lastWrite = 0;
    h = 0;
    n = 0;
    for (int i = 0; i < N; i++)
        lines[i][0] = 0;
}

time_t RamLog::LineIterator::lastWrite() {
    return _ramlog->_lastWrite;
}

long long RamLog::LineIterator::getTotalLinesWritten() {
    return _ramlog->_totalLinesWritten;
}

const char* RamLog::getLine_inlock(unsigned lineNumber) const {
    if (lineNumber >= n)
        return "";
    return lines[(lineNumber + h) % N];  // h = 0 unless n == N, hence modulo N.
}

int RamLog::repeats(const std::vector<const char*>& v, int i) {
    for (int j = i - 1; j >= 0 && j + 8 > i; j--) {
        if (strcmp(v[i] + 24, v[j] + 24) == 0) {
            for (int x = 1;; x++) {
                if (j + x == i)
                    return j;
                if (i + x >= (int)v.size())
                    return -1;
                if (strcmp(v[i + x] + 24, v[j + x] + 24))
                    return -1;
            }
            return -1;
        }
    }
    return -1;
}


string RamLog::clean(const std::vector<const char*>& v, int i, string line) {
    if (line.empty())
        line = v[i];
    if (i > 0 && strncmp(v[i], v[i - 1], 11) == 0)
        return string("           ") + line.substr(11);
    return v[i];
}

/* turn http:... into an anchor */
string RamLog::linkify(const char* s) {
    const char* p = s;
    const char* h = strstr(p, "http://");
    if (h == nullptr)
        return s;

    const char* sp = h + 7;
    while (*sp && *sp != ' ')
        sp++;

    string url(h, sp - h);
    std::stringstream ss;
    ss << string(s, h - s) << "<a href=\"" << url << "\">" << url << "</a>" << sp;
    return ss.str();
}

RamLog::LineIterator::LineIterator(RamLog* ramlog)
    : _ramlog(ramlog), _lock(ramlog->_mutex), _nextLineIndex(0) {}

RamLogAppender::RamLogAppender(RamLog* ramlog) : _ramlog(ramlog) {}
RamLogAppender::~RamLogAppender() {}

Status RamLogAppender::append(const logger::MessageEventEphemeral& event) {
    std::ostringstream ss;
    logger::MessageEventDetailsEncoder encoder;
    encoder.encode(event, ss);
    _ramlog->write(ss.str());
    return Status::OK();
}

// ---------------
// static things
// ---------------

RamLog* RamLog::get(const std::string& name) {
    if (!_namedLock) {
        // Guaranteed to happen before multi-threaded operation.
        _namedLock = new std::mutex();
    }

    stdx::lock_guard<std::mutex> lk(*_namedLock);
    if (!_named) {
        // Guaranteed to happen before multi-threaded operation.
        _named = new RM();
    }

    RamLog* result = mapFindWithDefault(*_named, name, static_cast<RamLog*>(nullptr));
    if (!result) {
        result = new RamLog(name);
        (*_named)[name] = result;
    }
    return result;
}

RamLog* RamLog::getIfExists(const std::string& name) {
    if (!_named)
        return nullptr;
    stdx::lock_guard<std::mutex> lk(*_namedLock);
    return mapFindWithDefault(*_named, name, static_cast<RamLog*>(nullptr));
}

void RamLog::getNames(std::vector<string>& names) {
    if (!_named)
        return;

    stdx::lock_guard<std::mutex> lk(*_namedLock);
    for (RM::iterator i = _named->begin(); i != _named->end(); ++i) {
        if (i->second->n)
            names.push_back(i->first);
    }
}

/**
 * Ensures that RamLog::get() is called at least once during single-threaded operation,
 * ensuring that _namedLock and _named are initialized safely.
 */
MONGO_INITIALIZER(RamLogCatalog)(InitializerContext*) {
    if (!_namedLock) {
        if (_named) {
            return Status(ErrorCodes::InternalError,
                          "Inconsistent intiailization of RamLogCatalog.");
        }
        _namedLock = new std::mutex();
        _named = new RM();
    }

    return Status::OK();
}
}  // namespace mongo
