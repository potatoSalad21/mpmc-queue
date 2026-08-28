#pragma once

#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <new>

#if defined(__cpp_lib_hardware_interference_size)
inline constexpr std::size_t kCacheLineSize =
    std::hardware_destructive_interference_size;
#else
inline constexpr std::size_t kCacheLineSize = 64;
#endif


template <typename T>
class MpmcBoundedQueue {
public:
    explicit MpmcBoundedQueue(std::size_t cap)
        : cap(cap), mask(cap-1) {

        if (cap < 2 || (cap & mask) != 0) {
            throw std::invalid_argument("MpmcBoundedQueue capacity must be a power of two >=2");
        }

        buffer = static_cast<Slot*>(
            ::operator new[](sizeof(Slot) * cap, std::align_val_t(alignof(Slot)))
        );

        for (std::size_t i = 0; i < cap; i++) {
            new (&buffer[i]) Slot();
            buffer[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~MpmcBoundedQueue() {
        std::size_t enq = enqueuePos.load(std::memory_order_relaxed);
        std::size_t deq = dequeuePos.load(std::memory_order_relaxed);

        for (std::size_t pos = deq; pos < enq; pos++) {
            buffer[pos & mask].storage.destroy();
        }

        for (std::size_t i = 0; i < cap; i++) {
            buffer[i].~Slot();
        }

        ::operator delete[](buffer, std::align_val_t(alignof(Slot)));
    }

    MpmcBoundedQueue(const MpmcBoundedQueue&) = delete;
    MpmcBoundedQueue& operator=(const MpmcBoundedQueue&) = delete;

    bool try_push(const T& val) {
        Slot *slot;
        std::size_t pos = enqueuePos.load(std::memory_order_relaxed);

        while (true) {
            slot = &buffer[pos & mask];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            std::intptr_t diff =
                static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);

            if (diff == 0) {
                if (enqueuePos.compare_exchange_weak(pos, pos+1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;   // full
            } else {
                pos = enqueuePos.load(std::memory_order_relaxed);
            }
        }

        slot->storage.construct(val);
        slot->sequence.store(pos+1, std::memory_order_release);

        return true;
    }

    bool try_push(T&& val) {
        Slot *slot;
        std::size_t pos = enqueuePos.load(std::memory_order_relaxed);

        while (true) {
            slot = &buffer[pos & mask];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            std::intptr_t diff =
                static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);

            if (diff == 0) {
                if (enqueuePos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = enqueuePos.load(std::memory_order_relaxed);
            }
        }

        slot->storage.construct(std::move(val));
        slot->sequence.store(pos + 1, std::memory_order_release);

        return true;
    }

    bool try_pop(T& out) {
        Slot *slot;
        std::size_t pos = dequeuePos.load(std::memory_order_relaxed);

        while (true) {
            slot = &buffer[pos & mask];
            std::size_t seq = slot->sequence.load(std::memory_order_acquire);
            std::intptr_t diff =
                static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);

            if (diff == 0) {
                if (dequeuePos.compare_exchange_weak(pos, pos+1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                return false;   // empty
            } else {
                pos = dequeuePos.load(std::memory_order_relaxed);
            }
        }

        out = slot->storage.get();
        slot->storage.destroy();
        slot->sequence.store(pos + cap, std::memory_order_release);

        return true;
    }

    std::size_t size_approx() const {
        std::size_t enq = enqueuePos.load(std::memory_order_relaxed);
        std::size_t deq = dequeuePos.load(std::memory_order_relaxed);

        return enq >= deq ? enq - deq : 0;
    }

    std::size_t capacity() const { return cap; }

private:
    struct RawStorage {
        alignas(T) std::byte bytes[sizeof(T)];

        void construct(const T& val) { new (bytes) T(val); }
        void construct(T&& val) { new (bytes) T(std::move(val)); }
        T& get() {
            return *std::launder(reinterpret_cast<T*>(bytes));
        }
        void destroy() { get().~T(); }
    };

    struct Slot {
        std::atomic<std::size_t> sequence;
        RawStorage storage;
    };

    const std::size_t cap;
    const std::size_t mask;
    Slot* buffer;

    alignas(kCacheLineSize) std::atomic<std::size_t> enqueuePos{ 0 };
    alignas(kCacheLineSize) std::atomic<std::size_t> dequeuePos{ 0 };
};
