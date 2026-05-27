#pragma once
#include <atomic>
#include <cassert>
#include <cstdio>
#include <mutex>
#include <thread>

// Drop-in replacement for std::mutex that catches:
//   - Same-thread double-lock (EDEADLK)
//   - Unlock from non-owner thread
// Active only when FELL_DEBUG_MUTEX is defined.
// Replace with plain std::mutex once the issue is found.

#ifdef FELL_DEBUG_MUTEX

struct DebugMutex {
    void lock() {
        auto tid = std::this_thread::get_id();
        if (owner_.load() == tid) {
            std::fprintf(stderr,
                "[DebugMutex] DOUBLE-LOCK on thread %zu — mutex already owned by this thread!\n",
                std::hash<std::thread::id>{}(tid));
            std::fflush(stderr);
            assert(false && "DebugMutex: same-thread double-lock detected");
        }
        mu_.lock();
        owner_.store(tid);
        lock_count_.fetch_add(1, std::memory_order_relaxed);
        std::fprintf(stderr,
            "[DebugMutex] LOCK   tid=%zu  total_locks=%zu\n",
            std::hash<std::thread::id>{}(tid),
            lock_count_.load(std::memory_order_relaxed));
        std::fflush(stderr);
    }

    void unlock() {
        auto tid = std::this_thread::get_id();
        if (owner_.load() != tid) {
            std::fprintf(stderr,
                "[DebugMutex] UNLOCK from non-owner thread %zu!\n",
                std::hash<std::thread::id>{}(tid));
            std::fflush(stderr);
            assert(false && "DebugMutex: unlock from non-owner thread");
        }
        owner_.store(std::thread::id{});
        mu_.unlock();
    }

    bool try_lock() {
        auto tid = std::this_thread::get_id();
        if (mu_.try_lock()) {
            owner_.store(tid);
            return true;
        }
        return false;
    }

private:
    std::mutex mu_;
    std::atomic<std::thread::id> owner_{};
    std::atomic<size_t> lock_count_{0};
};

#else
using DebugMutex = std::mutex;
#endif
