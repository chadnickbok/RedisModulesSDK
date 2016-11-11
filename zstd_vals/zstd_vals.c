#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"

#include <zstd.h>

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
    RedisModule_AutoMemory(ctx);


    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    int keytype = RedisModule_KeyType(key);
    if ((keytype != REDISMODULE_KEYTYPE_STRING) && (keytype != REDISMODULE_KEYTYPE_EMPTY))
    {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
  
    size_t len;
    const char *value = RedisModule_StringPtrLen(argv[2], &len);
    // XXX: Do I need to check for errors here? ie. zero-length values

    size_t bound = ZSTD_compressBound(len);
    void *buf = RedisModule_Alloc(bound);
    // XXX: Do I need to check return value of RedisModule_Alloc?

    size_t res = ZSTD_compress(buf, bound, value, len, 1); // Default super-fast mode
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
 * zstd_vals.ZSETLEVEL <key> <level> <value>
 */
int ZSETLEVELCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 4) 
    {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
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

    // open the key and make sure it's indeed a HASH and not empty
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
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
    RedisModule_ReplyWithStringBuffer(ctx, decompressed_buf, actual_size);

    RedisModule_Free(decompressed_buf);

    return REDISMODULE_OK;
}


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
    // zstd.ZDICT <key> <string> - upload a new compression dictionary with the given key
    // zstd.ZDICTSET <key> <dictkey> <string> - compress using the given dictionary
    // zstd.ZDICTSETLEVEL <key> <dictkey> <level> <string> - compress using the given dict and level
    // zstd.ZDICTGET <key> <dictkey> - get the value stored at key decompressed with the dictionary at dictkey

    return REDISMODULE_OK;
}
