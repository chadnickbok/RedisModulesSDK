#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"

#include <zstd.h>

/*
* example.HGETSET <key> <element> <value>
* Atomically set a value in a HASH key to <value> and return its value before
* the HSET.
*
* Basically atomic HGET + HSET
*/
int HGetSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // we need EXACTLY 4 arguments
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  // open the key and make sure it's indeed a HASH and not empty
  RedisModuleKey *key =
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH &&
      RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  // get the current value of the hash element
  RedisModuleCallReply *rep =
      RedisModule_Call(ctx, "HGET", "ss", argv[1], argv[2]);
  RMUTIL_ASSERT_NOERROR(rep);

  // set the new value of the element
  RedisModuleCallReply *srep =
      RedisModule_Call(ctx, "HSET", "sss", argv[1], argv[2], argv[3]);
  RMUTIL_ASSERT_NOERROR(srep);

  // if the value was null before - we just return null
  if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL) {
    RedisModule_ReplyWithNull(ctx);
    return REDISMODULE_OK;
  }

  // forward the HGET reply to the client
  RedisModule_ReplyWithCallReply(ctx, rep);
  return REDISMODULE_OK;
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

/**
 * zstd_vals.ZCHECK <key>
 * Confirm the given key is a valid ZSTD-compressed blob.
 */
int ZCHECKCommand() {
    return REDISMODULE_OK;
}

// Test the the PARSE command
int testParse(RedisModuleCtx *ctx) {

  RedisModuleCallReply *r =
      RedisModule_Call(ctx, "example.parse", "ccc", "SUM", "5", "2");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_INTEGER);
  RMUtil_AssertReplyEquals(r, "7");

  r = RedisModule_Call(ctx, "example.parse", "ccc", "PROD", "5", "2");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_INTEGER);
  RMUtil_AssertReplyEquals(r, "10");
  return 0;
}

// test the HGETSET command
int testHgetSet(RedisModuleCtx *ctx) {
  RedisModuleCallReply *r =
      RedisModule_Call(ctx, "example.hgetset", "ccc", "foo", "bar", "baz");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);

  r = RedisModule_Call(ctx, "example.hgetset", "ccc", "foo", "bar", "bag");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_STRING);
  RMUtil_AssertReplyEquals(r, "baz");
  r = RedisModule_Call(ctx, "example.hgetset", "ccc", "foo", "bar", "bang");
  RMUtil_AssertReplyEquals(r, "bag");
  return 0;
}

// Unit test entry point for the module
int TestModule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  RMUtil_Test(testParse);
  RMUtil_Test(testHgetSet);

  RedisModule_ReplyWithSimpleString(ctx, "PASS");
  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    // Register the module itself
    if (RedisModule_Init(ctx, "example", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    RMUtil_RegisterWriteCmd(ctx, "zstd.ZSET", ZSETCommand);
    RMUtil_RegisterReadCmd(ctx, "zstd.ZGET", ZGETCommand);
    // RMUtil_RegisterReadCmd(ctx, "zstd.ZCHECK", ZCHECKCommand);

    return REDISMODULE_OK;
}
