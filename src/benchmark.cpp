#include "queue.hpp"

#include <atomic>
#include <chrono>
#include <mutex>
#include <queue>
#include <thread>
#include <algorithm>
#include <iostream>

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

struct ThroughputResult {
    double ops_per_sec;
};

template <typename Adapter>
ThroughputResult run_throughput(
        int num_producers,
        int num_consumers,
        std::size_t capacity,
        std::chrono::milliseconds duration) {

    Adapter queue(capacity);
    std::atomic<bool> start{ false };
    std::atomic<bool> stop{ false };
    std::atomic<std::int64_t> total_pushed{ 0 };
    std::atomic<std::int64_t> total_popped{ 0 };

    auto producer = [&] {
        while (!start.load(std::memory_order_acquire))
            std::this_thread::yield();

        int val = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            if (queue.try_push(val)) {
                total_pushed.fetch_add(1, std::memory_order_relaxed);
                val++;
            }
        }
    };

    auto consumer = [&] {
        while (!start.load(std::memory_order_acquire))
            std::this_thread::yield();

        int val;
        while (!stop.load(std::memory_order_relaxed)) {
            if (queue.try_pop(val))
                total_popped.fetch_add(1, std::memory_order_relaxed);
        }

        while (queue.try_pop(val))
            total_popped.fetch_add(1, std::memory_order_relaxed);
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_producers; i++) threads.emplace_back(producer);
    for (int i = 0; i < num_consumers; i++) threads.emplace_back(consumer);

    start.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto t0 = Clock::now();
    std::this_thread::sleep_for(duration);
    stop.store(true, std::memory_order_relaxed);
    auto t1 = Clock::now();

    for (auto &t: threads) t.join();

    double elapsed_sec = std::chrono::duration<double>(t1 - t0).count();
    double ops = static_cast<double>(total_popped);

    return ThroughputResult{ ops / elapsed_sec };
}

struct LatencyResult {
    double p50_ns;
    double p99_ns;
    double p999_ns;
};

template <typename Adapter>
LatencyResult run_latency(std::size_t capacity, int num_samples) {
    Adapter queue(capacity);
    std::vector<std::int64_t> samples;
    samples.reserve(num_samples);

    std::atomic<bool> start{ false };
    std::atomic<bool> cons_done{ false };

    std::thread consumer_th([&] {
        while (!start.load(std::memory_order_acquire))
            std::this_thread::yield();

        int val;
        int received = 0;
        while (received < num_samples)
            if (queue.try_pop(val)) received++;

        cons_done.store(true, std::memory_order_release);
    });

    start.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    for (int i = 0; i < num_samples; i++) {
        auto t0 = Clock::now();
        while (!queue.try_push(i)) {
            // spin
        }
        auto t1 = Clock::now();
        samples.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }

    consumer_th.join();

    std::sort(samples.begin(), samples.end());
    auto pct = [&](double p) -> double {
        std::size_t idx = static_cast<std::size_t>(p * (samples.size() - 1));
        return static_cast<double>(samples[idx]);
    };

    return LatencyResult{pct(0.5), pct(0.99), pct(0.999)};
}

template <typename F>
auto repeat_collect(F&& fn, int repeats) {
    using ResultT = decltype(fn());
    std::vector<ResultT> results;
    for (int i = 0; i < repeats; i++)
        results.push_back(fn());

    return results;
}

void print_throughput_row(
        const std::string &queue_name,
        int producers, int consumers,
        const std::vector<ThroughputResult> &runs) {

    std::vector<double> vals;
    for (auto &r: runs)
        vals.push_back(r.ops_per_sec);
    std::sort(vals.begin(), vals.end());
    double median = vals[vals.size() / 2];

    std::cout << queue_name << "," << producers << "," << consumers << ","
              << "throughput," << vals.front() << "," << median << ","
              << vals.back() << "\n";
}

void print_latency_row(
        const std::string &queue_name,
        const std::vector<LatencyResult> &runs) {

    std::vector<double> p50s, p99s, p999s;
    for (auto &r : runs) {
        p50s.push_back(r.p50_ns);
        p99s.push_back(r.p99_ns);
        p999s.push_back(r.p999_ns);
    }
    std::sort(p50s.begin(), p50s.end());
    std::sort(p99s.begin(), p99s.end());
    std::sort(p999s.begin(), p999s.end());

    std::cout << queue_name << ",1,1,latency_p50_ns," << p50s.front() << ","
              << p50s[p50s.size() / 2] << "," << p50s.back() << "\n";
    std::cout << queue_name << ",1,1,latency_p99_ns," << p99s.front() << ","
              << p99s[p99s.size() / 2] << "," << p99s.back() << "\n";
    std::cout << queue_name << ",1,1,latency_p999_ns," << p999s.front() << ","
              << p999s[p999s.size() / 2] << "," << p999s.back() << "\n";
}

} // namespace

int main(int argc, char **argv) {
    constexpr std::size_t kQueueCapacity = 4096;
    constexpr int kRepeats = 5;
    const auto kThroughputDuration = std::chrono::milliseconds(1000);
    constexpr int kLatencySamples = 100000;

    unsigned hw_threads = std::thread::hardware_concurrency();
    std::cerr << "hardware threads: " << hw_threads;
    std::cerr << "\nkeep total threads at or below this, for meaningful resuls\n";

    std::cout << "queue,producers,consumers,metric,min,median,max\n";

    std::vector<int> thread_counts{ 1, 2, 4, 8 };
    if (argc > 1) {
        thread_counts.clear();
        for (int i = 1; i < argc; i++)
            thread_counts.push_back(std::atoi(argv[i]));
    }

    for (int n : thread_counts) {
        {
            auto runs = repeat_collect(
                [&] {
                    return run_throughput<MpmcAdapter<int>>(
                        n, n, kQueueCapacity, kThroughputDuration);
                },
                kRepeats
            );
            print_throughput_row("lockfree_mpmc", n, n, runs);
        }
        {
            auto runs = repeat_collect(
                [&] {
                    return run_throughput<MutexQueueAdapter<int>>(
                        n, n, kQueueCapacity, kThroughputDuration);
                },
                kRepeats
            );
            print_throughput_row("mutex_queue", n, n, runs);
        }
    }

    {
        auto runs = repeat_collect(
            [&] {
                return run_latency<MpmcAdapter<int>>(kQueueCapacity, kLatencySamples);
            },
            kRepeats
        );
        print_latency_row("lockfree_mpmc", runs);
    }
    {
        auto runs = repeat_collect(
            [&] {
                return run_latency<MutexQueueAdapter<int>>(kQueueCapacity, kLatencySamples);
            },
            kRepeats
        );
        print_latency_row("mutex_queue", runs);
    }

    return 0;
}
