/*
 * A minimal Redis module used only to prove the wrapper actually loads modules.
 *
 * It registers a name and one command and does nothing else. The point is to
 * be trivially buildable on any platform with a C compiler, so the module
 * loading path can be tested against a real `redis-server` without pulling in
 * a module framework or a second language toolchain.
 *
 * Build (needs redismodule.h from the matching Redis source tree):
 *
 *   cc -fPIC -shared -I<redis-src>/src tests/support/rsw_test_module.c \
 *      -o /tmp/rsw_test_module.so
 *
 * Then point the test suite at it:
 *
 *   REDIS_TEST_MODULE=/tmp/rsw_test_module.so cargo test --all-features
 *
 * Tests that need a module skip themselves when that variable is unset, so a
 * plain `cargo test` on a machine without a compiled module still passes.
 */

#include "redismodule.h"

/* RSW.ECHO <value> -- returns its argument.
 *
 * Exists so a test can prove the module is not merely listed but actually
 * serving commands. */
static int RswEcho_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc) {
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_ReplyWithString(ctx, argv[1]);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "rsw_test_module", 1, REDISMODULE_APIVER_1) ==
        REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "rsw.echo", RswEcho_RedisCommand,
                                  "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
