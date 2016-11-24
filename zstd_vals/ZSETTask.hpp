/**
 * Compress value and unblock client.
 */

#pragma once

#include "Task.hpp"

class ZSETTask : public Task {
public:
    ZSETTask();
    virtual ~ZSETTask();

    void Run() override;

    RedisModuleBlockedClient *bc;
    size_t key_len;
    char *key;
    size_t value_len;
    char *value;
    size_t res;
    void *compressed;
};
