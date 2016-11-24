/**
 * ZSTD redis module
 */

#include <memory>
#include <thread>

extern "C" {
#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"

#include <zstd.h>
}

#include "ZSETTask.hpp"
#include "TaskScheduler.hpp"

int ZSET_Reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    ZSETTask *task = (ZSETTask*) RedisModule_GetBlockedClientPrivateData(ctx);

    if (ZSTD_isError(task->res))
    {
        const char *zstd_err = ZSTD_getErrorName(task->res);
        return RedisModule_ReplyWithError(ctx, zstd_err);
    }

    RedisModuleString *keyname = RedisModule_CreateString(ctx, (const char*) task->key, task->key_len);

    RedisModuleKey *key = (RedisModuleKey *) RedisModule_OpenKey(ctx, keyname, REDISMODULE_READ|REDISMODULE_WRITE);
    int keytype = RedisModule_KeyType(key);
    if ((keytype != REDISMODULE_KEYTYPE_STRING) && (keytype != REDISMODULE_KEYTYPE_EMPTY))
    {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    // Update string with compressed data
    RedisModule_StringTruncate(key, task->res);
    size_t stringSize;
    char *stringDMA = (char *) RedisModule_StringDMA(key, &stringSize, REDISMODULE_READ | REDISMODULE_WRITE);
    memcpy(stringDMA, task->compressed, task->res);

    RedisModule_CloseKey(key);

    return RedisModule_ReplyWithSimpleString(ctx,"OK");
}

int ZSET_Timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    return RedisModule_ReplyWithSimpleString(ctx,"Request timedout");
}

void ZSET_FreeData(void *privdata)
{
    ZSETTask *task = (ZSETTask*) privdata;
    delete task;
}

/*
 * zstd_vals.ZSET <key> <value>
 * Compress and store a key.
 */
int ZSETCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    // we need exactly 3 arguments
    if (argc != 3)
    {
        return RedisModule_WrongArity(ctx);
    }

    ZSETTask *task = new ZSETTask();

    const char *key_in = RedisModule_StringPtrLen(argv[1], &task->key_len);
    task->key = (char*) RedisModule_Alloc(task->key_len);
    memcpy(task->key, key_in, task->key_len);

    const char *value_in = RedisModule_StringPtrLen(argv[2], &task->value_len);
    task->value = (char*) RedisModule_Alloc(task->value_len);
    memcpy(task->value, value_in, task->value_len);

    task->bc = RedisModule_BlockClient(ctx, ZSET_Reply, ZSET_Timeout, ZSET_FreeData, 1000 * 10);

    std::thread runtask(&ZSETTask::Run, task);
    runtask.detach();

    return REDISMODULE_OK;
}

/**
 * zstd_vals.ZSETLEVEL <key> <level> <value>
 */
int ZSETLEVELCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 4)
    {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    RedisModuleKey *key = (RedisModuleKey*) RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    int keytype = RedisModule_KeyType(key);
    if ((keytype != REDISMODULE_KEYTYPE_STRING) && (keytype != REDISMODULE_KEYTYPE_EMPTY))
    {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    long long level_arg;
    if (RedisModule_StringToLongLong(argv[2], &level_arg) != REDISMODULE_OK)
    {
        RedisModule_CloseKey(key);
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    int level = (int) level_arg;

    size_t len;
    const char *value = RedisModule_StringPtrLen(argv[3], &len);
    // XXX: Do I need to check for errors here? ie. zero-length values

    size_t bound = ZSTD_compressBound(len);
    void *buf = RedisModule_Alloc(bound);
    // XXX: Do I need to check return value of RedisModule_Alloc?

    size_t res = ZSTD_compress(buf, bound, value, len, level); // Default super-fast mode
    if (ZSTD_isError(res))
    {
        RedisModule_CloseKey(key);
        const char *zstd_err = ZSTD_getErrorName(res);
        return RedisModule_ReplyWithError(ctx, zstd_err);
    }

    RedisModuleString *compressed_string = RedisModule_CreateString(ctx, (const char *)buf, res);

    RedisModule_StringSet(key, compressed_string);
    RedisModule_CloseKey(key);
    RedisModule_Free(buf);

    RedisModule_ReplyWithSimpleString(ctx,"OK");
    return REDISMODULE_OK;
}

/*
 * zstd_vals.ZGET <key>
 * Get the raw value of a key and decompress it.
 */
int ZGETCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    // we need EXACTLY 2 arguments
    if (argc != 2)
    {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    // open the key and make sure it's indeed a string or empty
    RedisModuleKey *key = (RedisModuleKey*) RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING)
    {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    size_t len;
    char *compressed_buf = RedisModule_StringDMA(key, &len, REDISMODULE_READ);
    // XXX: Do I need to check for errors here?

    unsigned long long buf_size = ZSTD_getDecompressedSize(compressed_buf, len);

    void *decompressed_buf = RedisModule_Alloc(buf_size);
    // XXX: Errors?

    size_t actual_size = ZSTD_decompress(decompressed_buf, buf_size, compressed_buf, len);
    RedisModule_ReplyWithStringBuffer(ctx, (const char *) decompressed_buf, actual_size);

    RedisModule_Free(decompressed_buf);

    return REDISMODULE_OK;
}

/**
 * zstd.ZDICTSET <key> <dictkey> <value>
 * Set a compressed value using the dictionary stored at <dictkey>
 */
int ZDICTSETCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 4)
    {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    // open the key and make sure it's indeed a string or empty
    RedisModuleKey *key = (RedisModuleKey*) RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if ((RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING) && (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY))
    {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    // open the dictkey and make sure its a string
    RedisModuleKey *dictkey = (RedisModuleKey*) RedisModule_OpenKey(ctx, argv[2], REDISMODULE_READ);
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING)
    {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    // Get our dictionary
    size_t dict_len;
    char *dict_buf = RedisModule_StringDMA(dictkey, &dict_len, REDISMODULE_READ);

    size_t len;
    const char *value = RedisModule_StringPtrLen(argv[3], &len);
    // XXX: Do I need to check for errors here? ie. zero-length values

    size_t bound = ZSTD_compressBound(len);
    void *buf = RedisModule_Alloc(bound);
    // XXX: Do I need to check return value of RedisModule_Alloc?

    ZSTD_CCtx *zcctx = ZSTD_createCCtx();

    size_t res = ZSTD_compress_usingDict(zcctx, buf, bound, value, len, dict_buf, dict_len, 1);
    if (ZSTD_isError(res))
    {
        RedisModule_CloseKey(key);
        const char *zstd_err = ZSTD_getErrorName(res);
        return RedisModule_ReplyWithError(ctx, zstd_err);
    }

    RedisModuleString *compressed_string = RedisModule_CreateString(ctx, (const char *)buf, res);

    RedisModule_StringSet(key, compressed_string);
    RedisModule_CloseKey(key);
    RedisModule_Free(buf);

    RedisModule_ReplyWithSimpleString(ctx,"OK");

    return REDISMODULE_OK;
}

/**
 * zstd.ZDICTGET <key> <dictkey>
 * Get a compressed value stored at <key> and compressed with dictionary at <dictkey>
 */
int ZDICTGETCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    // we need EXACTLY 3 arguments
    if (argc != 3)
    {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    // ensure dictkey is string
    RedisModuleKey *dictkey = (RedisModuleKey*) RedisModule_OpenKey(ctx, argv[2], REDISMODULE_READ);
    if (RedisModule_KeyType(dictkey) != REDISMODULE_KEYTYPE_STRING)
    {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    // ensure key is string
    RedisModuleKey *key = (RedisModuleKey*) RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING)
    {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    // Get our dictionary
    size_t dict_len;
    char *dict_buf = RedisModule_StringDMA(dictkey, &dict_len, REDISMODULE_READ);

    size_t compressed_len;
    char *compressed_buf = RedisModule_StringDMA(key, &compressed_len, REDISMODULE_READ);
    // XXX: Do I need to check for errors here?

    unsigned long long buf_size = ZSTD_getDecompressedSize(compressed_buf, compressed_len);
    void *decompressed_buf = RedisModule_Alloc(buf_size);

    ZSTD_DCtx* zdctx = ZSTD_createDCtx();
    size_t actual_size = ZSTD_decompress_usingDict(
        zdctx,
        decompressed_buf, buf_size,
        compressed_buf, compressed_len,
        dict_buf, dict_len);
    RedisModule_ReplyWithStringBuffer(ctx, (const char*) decompressed_buf, actual_size);

    RedisModule_Free(decompressed_buf);

    return REDISMODULE_OK;
}

extern "C" {
int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    // Register the module itself
    if (RedisModule_Init(ctx, "example", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Basic commands
    RMUtil_RegisterWriteCmd(ctx, "zstd.ZSET", ZSETCommand);
    // zstd.ZSETLEVEL <key> <level> <string> - compress using the given level
    RMUtil_RegisterReadCmd(ctx, "zstd.ZGET", ZGETCommand);

    // Compress with level
    RMUtil_RegisterWriteCmd(ctx, "zstd.ZSETLEVEL", ZSETLEVELCommand);

    // Scary commands that can lose data (if a dict is lost so are all its compressed values)
    // zstd.ZDICTSET <key> <dictkey> <string> - compress using the given dictionary
    // zstd.ZDICTGET <key> <dictkey> - get the value stored at key decompressed with the dictionary at dictkey

    // The ultimate command
    // zstd.ZDICTSETLEVEL <key> <dictkey> <level> <string> - compress using the given dict and level

    return REDISMODULE_OK;
}
};
