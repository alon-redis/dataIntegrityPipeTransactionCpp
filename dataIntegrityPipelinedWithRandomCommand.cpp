#include <iostream>
#include <sstream>
#include <vector>
#include <thread>
#include <chrono>
#include <mutex>
#include <string>
#include <cstdlib>
#include <algorithm>
#include <ctime>
#include <fstream>
#include <hiredis/hiredis.h>

// Structure to hold key/value pairs for normal keys.
struct KV {
    std::string key;
    std::string value;
};

// Global vector (protected by a mutex) to store normal keys (for later verification).
std::vector<KV> globalData;
std::mutex globalDataMutex;

// (The global counter key is still created for consistency even though it isn’t used here.)
std::string globalCounterKey;

// Helper function to return a unique timestamp string.
std::string getCurrentTimestamp() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()).count();
    return std::to_string(ms);
}

// Helper function that reads all non-empty lines from /tmp/cmd.txt and returns one randomly.
std::string getRandomCommandFromFile() {
    std::ifstream file("/tmp/cmd.txt");
    if (!file) {
        // Fallback if file can't be opened.
        return "INFO";
    }
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty())
            lines.push_back(line);
    }
    file.close();
    if (lines.empty()) {
        return "INFO";
    }
    int idx = rand() % lines.size();
    return lines[idx];
}

// Connect to the Redis server.
redisContext* connectRedis(const std::string& host, int port) {
    redisContext* c = redisConnect(host.c_str(), port);
    if (c == nullptr || c->err) {
        if (c) {
            std::cout << "Connection error: " << c->errstr << std::endl;
            redisFree(c);
        }
        exit(1);
    }
    return c;
}

/**
 * Writer thread function.
 *
 * For each iteration:
 *   (1) Send a batch of SET commands for normal keys.
 *   (2) Append a random command from /tmp/cmd.txt.
 *       If that command begins with "SUBSCRIBE", append extra commands:
 *         - SUNSUBSCRIBE shardchannel1 shardchannel2
 *         - PUNSUBSCRIBE pattern pattern* p1 p2* p3*
 *         - UNSUBSCRIBE channel channel2 channel3
 *   (3) Read all replies (pipelineDepth replies for SET, one for the random command,
 *       plus extra replies if extra commands were appended).
 *   (4) Save the normal keys (and their expected values) in globalData.
 */
void writerThreadFunction(const std::string& host, int port,
                          int pipelineDepth, int iterations, int threadId)
{
    redisContext* conn = connectRedis(host, port);
    for (int iter = 0; iter < iterations; iter++) {
        struct Item {
            std::string normalKey, normalVal;
        };
        std::vector<Item> localBatch(pipelineDepth);
        
        // (1) Send SET commands.
        for (int i = 0; i < pipelineDepth; i++) {
            std::string timestamp = getCurrentTimestamp();
            std::stringstream nk;
            nk << "key:" << timestamp << ":" << threadId << ":" << iter << ":" << i;
            localBatch[i].normalKey = nk.str();
            localBatch[i].normalVal = "value:" + timestamp + ":" + std::to_string(threadId) + ":" +
                                      std::to_string(iter) + ":" + std::to_string(i);
            redisAppendCommand(conn, "SET %s %s",
                               localBatch[i].normalKey.c_str(),
                               localBatch[i].normalVal.c_str());
        }
        
        // (2) Append a random command.
        std::string randomCmd = getRandomCommandFromFile();
        std::cout << "[Writer " << threadId << "] Random command: " << randomCmd << std::endl;
        redisAppendCommand(conn, randomCmd.c_str());
        
        // If the random command starts with "SUBSCRIBE", append extra commands.
        int extraCommands = 0;
        if (randomCmd.find("SUBSCRIBE") == 0) {
            redisAppendCommand(conn, "SUNSUBSCRIBE shardchannel1 shardchannel2");
            redisAppendCommand(conn, "PUNSUBSCRIBE pattern pattern* p1 p2* p3*");
            redisAppendCommand(conn, "UNSUBSCRIBE channel channel2 channel3");
            extraCommands = 3;
        }
        
        // (3) Read pipeline replies.
        // a) Read SET command replies.
        for (int i = 0; i < pipelineDepth; i++) {
            redisReply* r = nullptr;
            if (redisGetReply(conn, (void**)&r) != REDIS_OK) {
                std::cout << "[Writer " << threadId << "] Error reading reply for SET command, err=" << conn->errstr << "\n";
            }
            if (r) freeReplyObject(r);
        }
        // b) Read random command reply.
        {
            redisReply* randomReply = nullptr;
            if (redisGetReply(conn, (void**)&randomReply) != REDIS_OK) {
                std::cout << "[Writer " << threadId << "] Error reading random command reply\n";
            }
            if (randomReply) freeReplyObject(randomReply);
        }
        // c) Read extra commands' replies (if any).
        for (int i = 0; i < extraCommands; i++) {
            redisReply* extraReply = nullptr;
            if (redisGetReply(conn, (void**)&extraReply) != REDIS_OK) {
                std::cout << "[Writer " << threadId << "] Error reading extra command reply\n";
            }
            if (extraReply) freeReplyObject(extraReply);
        }
        
        // (4) Save the normal keys for later verification.
        {
            std::lock_guard<std::mutex> lock(globalDataMutex);
            for (int i = 0; i < pipelineDepth; i++) {
                globalData.push_back({localBatch[i].normalKey, localBatch[i].normalVal});
            }
        }
    }
    redisFree(conn);
}

/**
 * Reader thread function.
 *
 * This function verifies that the normal keys contain the expected values.
 */
void readerThreadFunction(const std::string& host, int port,
                          int pipelineDepth, int threadId,
                          const std::vector<KV>& keysToRead)
{
    redisContext* conn = connectRedis(host, port);
    size_t index = 0;
    size_t totalKeys = keysToRead.size();
    while (index < totalKeys) {
        size_t batchSize = std::min((size_t)pipelineDepth, totalKeys - index);
        for (size_t i = 0; i < batchSize; i++) {
            const KV& kv = keysToRead[index + i];
            redisAppendCommand(conn, "GET %s", kv.key.c_str());
        }
        for (size_t i = 0; i < batchSize; i++) {
            redisReply* r = nullptr;
            if (redisGetReply(conn, (void**)&r) != REDIS_OK)
                std::cout << "[Reader " << threadId << "] Error reading GET reply\n";
            if (r) {
                std::string value = (r->type == REDIS_REPLY_STRING && r->str) ? r->str : "";
                if (value != keysToRead[index + i].value) {
                    std::cout << "[Reader " << threadId << "] Data mismatch for key=" << keysToRead[index + i].key
                              << ", expected=" << keysToRead[index + i].value
                              << ", got=" << value << "\n";
                }
                freeReplyObject(r);
            }
        }
        index += batchSize;
    }
    redisFree(conn);
}

/**
 * Single-connection mode.
 *
 * This function performs both write and read phases on a single connection.
 */
void singleConnectionFunction(const std::string& host, int port,
                              int pipelineDepth, int iterations)
{
    // Write phase.
    {
        redisContext* conn = connectRedis(host, port);
        for (int iter = 0; iter < iterations; iter++) {
            struct Item {
                std::string normalKey, normalVal;
            };
            std::vector<Item> localBatch(pipelineDepth);
            for (int i = 0; i < pipelineDepth; i++) {
                std::string timestamp = getCurrentTimestamp();
                std::stringstream nk;
                nk << "key:" << timestamp << ":0:" << iter << ":" << i;
                localBatch[i].normalKey = nk.str();
                localBatch[i].normalVal = "value:" + timestamp + ":0:" +
                                          std::to_string(iter) + ":" + std::to_string(i);
                redisAppendCommand(conn, "SET %s %s",
                                   localBatch[i].normalKey.c_str(),
                                   localBatch[i].normalVal.c_str());
            }
            // Append a random command.
            std::string randomCmd = getRandomCommandFromFile();
            std::cout << "[SingleConn] Random command: " << randomCmd << std::endl;
            redisAppendCommand(conn, randomCmd.c_str());
            int extraCommands = 0;
            if(randomCmd.find("SUBSCRIBE") == 0) {
                redisAppendCommand(conn, "SUNSUBSCRIBE shardchannel1 shardchannel2");
                redisAppendCommand(conn, "PUNSUBSCRIBE pattern pattern* p1 p2* p3*");
                redisAppendCommand(conn, "UNSUBSCRIBE channel channel2 channel3");
                extraCommands = 3;
            }
            // Read replies.
            for (int i = 0; i < pipelineDepth; i++) {
                redisReply* r = nullptr;
                if (redisGetReply(conn, (void**)&r) != REDIS_OK)
                    std::cout << "[SingleConn] Error reading reply for SET\n";
                if (r) freeReplyObject(r);
            }
            {
                redisReply* randomReply = nullptr;
                if (redisGetReply(conn, (void**)&randomReply) != REDIS_OK)
                    std::cout << "[SingleConn] Error reading random command reply\n";
                if (randomReply) freeReplyObject(randomReply);
            }
            for (int i = 0; i < extraCommands; i++) {
                redisReply* extraReply = nullptr;
                if (redisGetReply(conn, (void**)&extraReply) != REDIS_OK)
                    std::cout << "[SingleConn] Error reading extra command reply\n";
                if (extraReply) freeReplyObject(extraReply);
            }
            {
                std::lock_guard<std::mutex> lock(globalDataMutex);
                for (int i = 0; i < pipelineDepth; i++) {
                    globalData.push_back({localBatch[i].normalKey, localBatch[i].normalVal});
                }
            }
        }
        redisFree(conn);
    }
    // Read phase.
    {
        std::vector<KV> localData;
        {
            std::lock_guard<std::mutex> lock(globalDataMutex);
            localData = globalData;
        }
        redisContext* conn = connectRedis(host, port);
        size_t index = 0;
        size_t totalKeys = localData.size();
        while (index < totalKeys) {
            size_t batchSize = std::min((size_t)pipelineDepth, totalKeys - index);
            for (size_t i = 0; i < batchSize; i++) {
                redisAppendCommand(conn, "GET %s", localData[index + i].key.c_str());
            }
            for (size_t i = 0; i < batchSize; i++) {
                redisReply* r = nullptr;
                if (redisGetReply(conn, (void**)&r) != REDIS_OK)
                    std::cout << "[SingleConn] Error reading GET reply in read phase\n";
                if (r) {
                    std::string value = (r->type == REDIS_REPLY_STRING && r->str) ? r->str : "";
                    if (value != localData[index + i].value) {
                        std::cout << "[SingleConn] Data mismatch for key=" << localData[index + i].key
                                  << ", expected=" << localData[index + i].value
                                  << ", got=" << value << "\n";
                    }
                    freeReplyObject(r);
                }
            }
            index += batchSize;
        }
        redisFree(conn);
    }
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cout << "Usage: " << argv[0]
                  << " <redis_host:port> <pipeline_depth> <num_connections> <iterations>\n";
        return 1;
    }
    // Initialize random seed.
    srand(time(NULL));

    std::string hostPort = argv[1];
    int pipelineDepth = std::stoi(argv[2]);
    int numConnections = std::stoi(argv[3]);
    int iterations = std::stoi(argv[4]);
    std::string host;
    int port;
    size_t pos = hostPort.find(":");
    if (pos != std::string::npos) {
        host = hostPort.substr(0, pos);
        port = std::stoi(hostPort.substr(pos + 1));
    } else {
        host = hostPort;
        port = 6379;
    }

    // Create a global counter key with a random component.
    globalCounterKey = "ctr:" + getCurrentTimestamp() + "_" + std::to_string(rand());
    std::cout << "Global counter key: " << globalCounterKey << "\n";

    if (numConnections == 1) {
        std::cout << "[Single-connection mode]\n";
        singleConnectionFunction(host, port, pipelineDepth, iterations);
        std::cout << "Single-thread data validation completed.\n";
    } else {
        int numWriters = (numConnections + 1) / 2;
        int numReaders = numConnections - numWriters;
        std::cout << "Num connections: " << numConnections
                  << " => Writers: " << numWriters
                  << ", Readers: " << numReaders << std::endl;
        std::vector<std::thread> writerThreads;
        writerThreads.reserve(numWriters);
        for (int i = 0; i < numWriters; i++) {
            writerThreads.emplace_back(writerThreadFunction, host, port, pipelineDepth, iterations, i);
        }
        for (auto &t : writerThreads) {
            t.join();
        }
        {
            std::lock_guard<std::mutex> lock(globalDataMutex);
            std::cout << "Total normal keys written: " << globalData.size() << std::endl;
        }
        std::vector<std::vector<KV>> readerPartitions(numReaders);
        {
            std::lock_guard<std::mutex> lock(globalDataMutex);
            for (size_t i = 0; i < globalData.size(); i++) {
                readerPartitions[i % numReaders].push_back(globalData[i]);
            }
        }
        std::vector<std::thread> readerThreads;
        readerThreads.reserve(numReaders);
        for (int i = 0; i < numReaders; i++) {
            readerThreads.emplace_back(readerThreadFunction, host, port, pipelineDepth, i, readerPartitions[i]);
        }
        for (auto &t : readerThreads) {
            t.join();
        }
        std::cout << "Data validation (normal keys) completed.\n";
    }

    // Final verification: send a random command (from /tmp/cmd.txt) as part of the pipeline.
    redisContext* conn = connectRedis(host, port);
    std::string finalCmd = getRandomCommandFromFile();
    std::cout << "[Final] Random command: " << finalCmd << std::endl;
    redisAppendCommand(conn, finalCmd.c_str());
    int extraCommands = 0;
    if(finalCmd.find("SUBSCRIBE") == 0) {
        redisAppendCommand(conn, "SUNSUBSCRIBE shardchannel1 shardchannel2");
        redisAppendCommand(conn, "PUNSUBSCRIBE pattern pattern* p1 p2* p3*");
        redisAppendCommand(conn, "UNSUBSCRIBE channel channel2 channel3");
        extraCommands = 3;
    }
    {
        redisReply* r = nullptr;
        if (redisGetReply(conn, (void**)&r) != REDIS_OK)
            std::cout << "[Final] Error reading final random command reply\n";
        if (r) freeReplyObject(r);
    }
    for (int i = 0; i < extraCommands; i++) {
        redisReply* r = nullptr;
        if (redisGetReply(conn, (void**)&r) != REDIS_OK)
            std::cout << "[Final] Error reading extra command reply\n";
        if (r) freeReplyObject(r);
    }
    redisFree(conn);

    return 0;
}
