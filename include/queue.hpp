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
        : capacity(cap), mask(capacity-1) {

        if (capacity < 2 || (capacity & mask) != 0) {
            throw std::invalid_argument("MpmcBoundedQueue capacity must be a power of two >=2");
        }

        buffer = static_cast<Slot*>(
            ::operator new[](sizeof(Slot) * capacity, std::align_val_t(alignof(Slot)))
        );

        for (std::size_t i = 0; i < capacity; i++) {
            new (&buffer[i]) Slot();
            buffer[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~MpmcBoundedQueue() {
        for (std::size_t i = 0; i < capacity; i++) {
            buffer[i].~Slot();
        }

        ::operator delete[](buffer, std::align_val_t(alignof(Slot)));
    }

    MpmcBoundedQueue(const MpmcBoundedQueue&) = delete;
    MpmcBoundedQueue& operator=(const MpmcBoundedQueue&) = delete;

private:
    struct RawStorage {
        alignas(T) std::byte bytes[sizeof(T)];

        void construct(const T& val) { new (bytes) T(val); }
        void construct(const T&& val) { new (bytes) T(std::move(val)); }
        void get() {
            return std::launder(reinterpret_cast<T*>(bytes));
        }
        void destroy() { get().~T(); }
    };

    struct Slot {
        std::atomic<std::size_t> sequence;
        RawStorage storage;
    };

    const std::size_t capacity;
    const std::size_t mask;
    Slot* buffer;

    alignas(kCacheLineSize) std::atomic<std::size_t> enqueuePos{ 0 };
    alignas(kCacheLineSize) std::atomic<std::size_t> dequeuePos{ 0 };
};
