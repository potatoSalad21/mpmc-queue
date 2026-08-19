#include <new>
#include <cstddef>
#include <atomic>

#if defined(__cpp_lib_hardware_interference_size)
inline constexpr std::size_t kCacheLineSize =
    std::hardware_destructive_interference_size;
#else
inline constexpr std::size_t kCacheLineSize = 64;
#endif

template <typename T>
class MpmcBoundedQueue {
public:

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
