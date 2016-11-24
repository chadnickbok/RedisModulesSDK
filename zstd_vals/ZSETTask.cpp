/**
 * Abstract Task class.
 * Override Run in sub-classes.
 */

#include "ZSETTask.hpp"

extern "C" {
#include <zstd.h>
#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"
}

ZSETTask::ZSETTask()
{
    ;
}

ZSETTask::~ZSETTask()
{
    ;
}

void ZSETTask::Run()
{
    size_t bound = ZSTD_compressBound(this->value_len);
    this->compressed = RedisModule_Alloc(bound);
    this->res = ZSTD_compress(this->compressed, bound, this->value, this->value_len, 1);

    RedisModule_UnblockClient(this->bc, task);
}
