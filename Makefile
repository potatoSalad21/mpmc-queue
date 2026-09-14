CXX := g++
STD := -std=c++17
CPPFLAGS := -Iinclude

TSAN_FLAGS := $(STD) -fsanitize=thread -g -O1 -fno-omit-frame-pointer
RELEASE_FLAGS := $(STD) -O3 -g

LDLIBS := -lpthread

BIN_DIR := bin

.PHONY: all test benchmark run-test run-benchmark clean

all: test benchmark

benchmark: $(BIN_DIR)/benchmark
test: $(BIN_DIR)/test_stress

$(BIN_DIR)/benchmark: src/benchmark.cpp include/queue.hpp | $(BIN_DIR)
	$(CXX) $(RELEASE_FLAGS) $(CPPFLAGS) $< -o $@ $(LDLIBS)

$(BIN_DIR)/test_stress: src/mpmc_test.cpp include/queue.hpp | $(BIN_DIR)
	$(CXX) $(TSAN_FLAGS) $(CPPFLAGS) $< -o $@ $(LDLIBS)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

run-test: test
	./$(BIN_DIR)/test_stress

run-benchmark: benchmark
	./$(BIN_DIR)/benchmark | tee results.csv

clean:
	rm -rf $(BIN_DIR)/* results.csv
