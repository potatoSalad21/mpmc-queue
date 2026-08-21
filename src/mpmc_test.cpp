#include <queue.hpp>

#include <iostream>
#include <cstdlib>
#include <vector>
#include <atomic>
#include <thread>

constexpr std::size_t capacity = 32;
constexpr int producerNum = 8;
constexpr int consumerNum = 8;
constexpr int itemsPerProducer = 20000;
constexpr int totalItems = itemsPerProducer * producerNum;

struct Item {
    int value;
};

void producer_fn(MpmcBoundedQueue<Item> &queue, int id, std::atomic<bool> &startFlag) {

}

void consumer_fn(MpmcBoundedQueue<Item> &queue,
                 std::atomic<bool> &startFlag,
                 std::atomic<int> &consumedCnt,
                 std::vector<std::atomic<bool>> &seen) {

}

bool run_once(int runIdx) {
    MpmcBoundedQueue<Item> q(capacity);
    std::atomic<bool> startFlag{ false };
    std::atomic<int> consumedCnt{ 0 };

    std::vector<std::atomic<bool>> seen(totalItems);
    for (auto &flag : seen) {
        flag.store(false, std::memory_order_relaxed);
    }

    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    producers.reserve(producerNum);
    consumers.reserve(consumerNum);

    for (int p = 0; p < producerNum; p++) {
        producers.emplace_back(producer_fn, std::ref(q), p, std::ref(startFlag));
    }
    for (int c = 0; c < consumerNum; c++) {
        consumers.emplace_back(
                consumer_fn,
                std::ref(q),
                std::ref(startFlag),
                std::ref(consumedCnt),
                std::ref(seen)
        );
    }

    startFlag.store(true, std::memory_order_release);

    for (auto &t : producers) t.join();
    for (auto &t : consumers) t.join();

    int missing = 0;
    for (int i = 0; i < totalItems; i++) {
        if (!seen[i].load(std::memory_order_relaxed)) {
            missing++;
            if (missing <= 10) {
                std::cerr << "[FATAL] value " << i << " was never popped\n";
            }
        }
    }

    if (missing > 0) {
        std::cerr << "Run " << runIdx << ": " << missing << " item(s) lost. FAIL\n";
        return false;
    }

    if (consumedCnt.load(std::memory_order_relaxed) != totalItems) {
        std::cerr << "Run " << runIdx << ": consumed count mismatch. FAIL\n";
        return false;
    }

    std::cout << "Run " << runIdx << ": OK (" << totalItems
              << " items, " << producerNum << "P / " << consumerNum
              << "C, capacity=" << capacity << ")\n";

    return true;
}

int main(int argc, char **argv) {
    int runs = 5;
    if (argc > 1) {
	    runs = std::stoi(argv[1]);
    }

    for (int i = 0; i < runs; i++) {
	    if (!run_once(i)) {
	        std::cerr << "stress test failed on run " << i << '\n';
	        return 1;
	    }
    }
    std::cout << "all " << runs << " runs passed.\n";

    return 0;
}
