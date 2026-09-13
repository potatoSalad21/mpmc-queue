#include "queue.hpp"

#include <chrono>
#include <mutex>
#include <queue>

namespace {

using Clock = std::chrono::steady_clock;

template <typename T>
class MpmcAdapter {
public:
    explicit MpmcAdapter(std::size_t capacity) : queue_(capacity) { }

    bool try_push(const T& val) { return queue_.try_push(val); }
    bool try_pop(T& out) { return queue_.try_pop(out); }

    static constexpr const char *name() { return "lockfree_mpmc"; }

private:
    MpmcBoundedQueue<T> queue_;
};

template <typename T>
class MutexQueueAdapter {
public:
    explicit MutexQueueAdapter(std::size_t capacity) : capacity_(capacity) { }

    bool try_push(const T& val) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= capacity_) return false;
        queue_.push(val);

        return true;
    }

    bool try_pop(T& out) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return false;
        out = queue_.front();
        queue_.pop();

        return true;
    }

    static constexpr const char *name() { return "mutex_queue"; }

private:
    std::mutex mutex_;
    std::queue<T> queue_;
    std::size_t capacity_;
};

} // namespace

int main(int argc, char **argv) {

    return 0;
}
