#include <signal.h> // For redis.h 'siginfo_t'
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dlfcn.h>
#include <ctype.h>
#undef __GNUC__  // allow malloc (needed for uthash)  (see redis.h ln 1403)
#include "uthash.h"
#include "kvolve.h"
#include "redis.h"
#include "kvolve_upd.h"
#include "kvolve_internal.h"

struct kvolve_cmd_hash_populate kvolveCommandTable[] = {
    {"set",kvolve_set,3},
    {"mset",kvolve_mset,3},
    {"get",kvolve_get,2},
    {"mget",kvolve_mget,2},
    {"getset",kvolve_getset,3},
    {"getrange",kvolve_getrange,4},
    {"incr",kvolve_incr,2},
    {"incrby",kvolve_incrby,3},
    {"del",kvolve_del,2},
    {"hdel",kvolve_hdel,3},
    {"hget",kvolve_hget,3},
    {"hgetall",kvolve_hgetall,2},
    {"hset",kvolve_hset,4},
    {"hmset",kvolve_hmset,4},
    {"hmget",kvolve_hmget,3},
    {"lrange",kvolve_lrange,4},
    {"lpush",kvolve_lpush,3},
    {"lpop",kvolve_lpop,2},
    {"llen",kvolve_llen,2},
    {"lset",kvolve_lset,2},
    {"rpush",kvolve_rpush,3},
    {"rpop",kvolve_rpop,2},
    {"setnx",kvolve_setnx,3},
    {"sadd",kvolve_sadd,3},
    {"scard",kvolve_scard,3},
    {"spop",kvolve_spop,2},
    {"smembers",kvolve_smembers,2},
    {"sismember",kvolve_sismember,3},
    {"srem",kvolve_srem,3},
    {"zadd",kvolve_zadd,4},
    {"zcard",kvolve_zcard,2},
    {"zscore",kvolve_zscore,3},
    {"zrem",kvolve_zrem,3},
    {"zrange",kvolve_zrange,4},
    {"zrevrange",kvolve_zrevrange,4},
    {"keys",kvolve_keys,2}
};
struct kvolve_cmd_hash * kvolve_commands = NULL;

void kvolve_process_command(redisClient *c){

    /* preserve these */
    long long n = server.stat_numcommands;
    long long h = server.stat_keyspace_hits;
    long long m = server.stat_keyspace_misses;

    kvolve_prevcall_check();

    if (c->argc == 3 && (strcasecmp((char*)c->argv[0]->ptr, "client") == 0)
            && (strcasecmp((char*)c->argv[1]->ptr, "setname") == 0)
            && (strncasecmp((char*)c->argv[2]->ptr, "update", 6) == 0)){
        kvolve_load_update(c, (char*)(c->argv[2]->ptr)+6);
    } else if (c->argc == 3 && (strcasecmp((char*)c->argv[0]->ptr, "client") == 0) && 
            (strcasecmp((char*)c->argv[1]->ptr, "setname") == 0)){
        kvolve_check_version(c);
    } else if (c->argc == 2 && (strcasecmp((char*)c->argv[0]->ptr, "keys") == 0)){
        kvolve_keys(c, NULL);
    } else if(c->argc > 1) {
        /* if no namespace on key, lookup will return version '*' if global ns
         * requested by client, else NULL*/
        struct version_hash * v = kvolve_version_hash_lookup((char*)c->argv[1]->ptr);
        if(!v) 
            return;
        kvolve_call fun = kvolve_lookup_kv_command(c);
        if(!fun){
            DEBUG_PRINT(("Function %s not implemented\n", (char*)c->argv[0]->ptr));
            return;
        }
        fun(c, v);
    }
    /* restore */
    server.stat_numcommands = n;
    server.stat_keyspace_hits = h;
    server.stat_keyspace_misses = m;
}

void kvolve_set(redisClient * c, struct version_hash * v){

    int flags;

    if(v == NULL) return;

    /* check to see if any xx/nx flags set */
    flags = kvolve_get_flags(c);
    if(flags & REDIS_SET_XX){
        kvolve_setxx(c, v);
        return;
    }
    if(flags & REDIS_SET_NX){
        kvolve_setnx(c, v);
        return;
    }


    /* Set the version field in the value (only the string is stored for the
     * key).  Note that this will automatically blow away any old version. */
    c->argv[2]->vers = v->versions[v->num_versions-1];

    /* Since there are no (nx,xx) flags, the set will occur.
     * Check to see if it's possible that an old version exists
     * under another namespace that should be deleted. */
    if(v->prev_ns != NULL && !dictFind(c->db->dict,c->argv[1]->ptr)){
        kvolve_checkdel_old(c, v);
    }
}

/* multi-set ... loop over all args */
void kvolve_mset(redisClient * c, struct version_hash * v){
    int i;
    redisClient * c_fake = createClient(-1);
    c_fake->db = c->db;
    c_fake->argc = 3;
    c_fake->argv = zmalloc(sizeof(void*)*3);

    assert(c->argc % 2 == 1);
    for (i=1; i < c->argc; i=i+2){
        c_fake->argv[1]= c->argv[i];
        c_fake->argv[2]= c->argv[i+1];
        kvolve_set(c_fake, v);
    }
    zfree(c_fake->argv);
    zfree(c_fake);
}

void kvolve_get(redisClient * c, struct version_hash * v){
    kvolve_check_update_kv_pair(c, v, 1, NULL, REDIS_STRING, NULL, NULL);
}

/* NX -- Only set the key if it does not already exist*/
void kvolve_setnx(redisClient * c, struct version_hash * v){

    /* Do nothing if already at current namespace, do nothing*/
    if (lookupKey(c->db, c->argv[1]))
        return;

    robj * present = kvolve_get_db_val(c, v);
    DEBUG_PRINT(("Present is = %p\n", (void*)present));
    /* If doesn't exist anywhere, do nothing */
    if (present == NULL)
        return;

    /* But if the key DOES exist at a PRIOR namespace, then we need to
     * rename the key, so that the set doesn't erroneously occur (because
     * it will appear to be fake-missing because it is under the old name.
     *     (Note that the set will not occur!!!) 
     * This leaves the version number at the old, so when a set _does_ occur,
     * the version will be bumped up only at that time. */
    kvolve_namespace_update(c,v);

}

/* XX -- Only set the key if it already exists. */
void kvolve_setxx(redisClient * c, struct version_hash * v){

    /* we can reuse the basics which just renames if necessary*/
    kvolve_setnx(c, v);

    /* If the set occurs, this will correctly bump the version.  If doesn't
     * occur, this will be ignored.*/
    c->argv[2]->vers = v->versions[v->num_versions-1];
}

/* We only have to worry about namespace changes here. We need to do the rename
 * so it will be deleted properly (and return the right value count to the client)*/
void kvolve_del(redisClient * c, struct version_hash * v){
    kvolve_check_rename(c, v, c->argc);
}

/* check for update, the same as kvolve_get, but a substring */
void kvolve_getrange(redisClient *c, struct version_hash * v){
    kvolve_get(c, v);
}

/* Most of the following functions are effectively "gets". This
 * will just check for update, and do if necessary. Remember, we must keep
 * all set/zset/etc members at same version. this will do that. */
void kvolve_sismember(redisClient * c, struct version_hash * v){
    kvolve_update_all_set(c, v);
}
void kvolve_srem(redisClient * c, struct version_hash * v){
    kvolve_update_all_set(c, v);
}
void kvolve_scard(redisClient * c, struct version_hash * v){
    kvolve_update_all_set(c, v);
}
void kvolve_spop(redisClient * c, struct version_hash * v){
    kvolve_update_all_set(c, v);
}
void kvolve_smembers(redisClient * c, struct version_hash * v){
    kvolve_update_all_set(c, v);
}
void kvolve_incrby(redisClient * c, struct version_hash * v){
    kvolve_incr(c, v);
}
void kvolve_getset(redisClient * c, struct version_hash * v){
    kvolve_get(c, v);
}
void kvolve_zcard(redisClient * c, struct version_hash * v){
    kvolve_update_all_zset(c, v);
}
void kvolve_zrem(redisClient * c, struct version_hash * v){
    kvolve_update_all_zset(c, v);
}
void kvolve_zscore(redisClient * c, struct version_hash * v){
    kvolve_update_all_zset(c, v);
}
void kvolve_zrange(redisClient * c, struct version_hash * v){
    kvolve_update_all_zset(c, v);
}
void kvolve_zrevrange(redisClient * c, struct version_hash * v){
    kvolve_update_all_zset(c, v);
}
void kvolve_lrange(redisClient * c, struct version_hash * v){
    kvolve_update_all_list(c, v);
}
void kvolve_lpop(redisClient * c, struct version_hash * v){
    kvolve_update_all_list(c, v);
}
void kvolve_rpop(redisClient * c, struct version_hash * v){
    kvolve_update_all_list(c, v);
}
void kvolve_rpush(redisClient * c, struct version_hash * v){
    kvolve_lpush(c, v);
}
void kvolve_llen(redisClient * c, struct version_hash * v){
    kvolve_update_all_list(c, v);
}
void kvolve_lset(redisClient * c, struct version_hash * v){
    kvolve_update_all_list(c, v);
}

/* the incr command blows away any version you pass it, because it creates its
 * own object.  Since incr values can ONLY be strings that convert to ints,
 * there is no way that there can be a meaningful value change.  Therefore, we
 * just need to make sure the name is current and go from there. */
void kvolve_incr(redisClient * c, struct version_hash * v){

    if(!v || !v->prev_ns) return;

    /* check if current at correct ns, or doesn't exist at all*/
    if(dictFind(c->db->dict, c->argv[1]) || (kvolve_get_db_val(c, v)==NULL))
        return;

    /* at this point, we must update the namespace */
    kvolve_namespace_update(c, v);

}

void kvolve_mget(redisClient * c, struct version_hash * v){
    int i;
    redisClient * c_fake = createClient(-1);
    c_fake->db = c->db;
    c_fake->argc = 2;
    c_fake->argv = zmalloc(sizeof(void*)*2);

    for (i=1; i < c->argc; i++){
        c_fake->argv[1]= c->argv[i];
        kvolve_get(c_fake, v);
    }
    zfree(c_fake->argv);
    zfree(c_fake);
}

void kvolve_hmget(redisClient * c, struct version_hash * v){
    kvolve_update_all_hash(c, v);
}



void kvolve_sadd(redisClient * c, struct version_hash * v){
    int ret = kvolve_update_all_set(c, v);
    if (ret == 1)
        kvolve_new_version(c, v);
}

/* similar to sadd but for sorted sets */
void kvolve_zadd(redisClient * c, struct version_hash * v){
    /* Make sure all set elements are at this current version. Else update all*/
    int ret = kvolve_update_all_zset(c, v);
    if (ret == 1)
        kvolve_new_version(c, v);
}

void kvolve_lpush(redisClient * c, struct version_hash * v){
    int ret = kvolve_update_all_list(c, v);
    if (ret == 1)
        kvolve_new_version(c, v);
}

void kvolve_hmset(redisClient * c, struct version_hash * v){
    int ret = kvolve_update_all_hash(c, v);
    if (ret == 1)
        kvolve_new_version(c, v);
}
void kvolve_hset(redisClient * c, struct version_hash * v){
    kvolve_hmset(c, v);
} 

void kvolve_hget(redisClient * c, struct version_hash * v){
    kvolve_update_all_hash(c, v);
}

void kvolve_hdel(redisClient * c, struct version_hash * v){
    kvolve_update_all_hash(c, v);
}

void kvolve_hgetall(redisClient * c, struct version_hash * v){
    kvolve_update_all_hash(c, v);
} 

void kvolve_keys(redisClient * c, struct version_hash * v){
    dictIterator *di;
    dictEntry *de;
    char * new;
    sds pattern = c->argv[1]->ptr, tmp;
    int plen = sdslen(pattern);
    /* escape hatch, if caller requests 'keys kvolve', this doesn't update */
    if(strcmp("kvolve", pattern) == 0){
        sdsfree(pattern);
        c->argv[1]->ptr = sdsnew("*");
        return;
    }
    di = dictGetSafeIterator(c->db->dict);
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        v = kvolve_version_hash_lookup((char*)key);
        if(v && v->next_ns){
            new = kvolve_construct_prev_name(key, v->next_ns);
            tmp = sdsnew(new); 
            if(stringmatchlen(pattern,plen,tmp,sdslen(tmp),0)){
                c->argv[1]->ptr = tmp;
                v = kvolve_version_hash_lookup((char*)new);
                /* This will only update the namespace.  The value will be
                 * updated if the key is queried (version is not bumped) */
                kvolve_namespace_update(c, v);
            }
            free(new);
            sdsfree(tmp);
        }
    }
    dictReleaseIterator(di);
    /* restore */
    c->argv[1]->ptr = pattern;
}

void kvolve_populateCommandTable(void){
    int j, i;
    char * ucase;
    int numcommands = sizeof(kvolveCommandTable)/sizeof(struct kvolve_cmd_hash_populate);

    for (j = 0; j < numcommands; j++) {
        struct kvolve_cmd_hash_populate *c = kvolveCommandTable+j;
        struct kvolve_cmd_hash * c_h = malloc(sizeof(struct kvolve_cmd_hash));
        struct kvolve_cmd_hash * c_hU = malloc(sizeof(struct kvolve_cmd_hash));
        c_h->cmd = c->cmd;
        c_h->call = c->call;
        c_h->min_args = c->min_args;
        HASH_ADD_KEYPTR(hh, kvolve_commands, c_h->cmd, strlen(c_h->cmd), c_h);
        /* also add upper case */
        ucase = calloc(strlen(c->cmd), sizeof(char));
        c_hU->call = c->call;
        c_hU->min_args = c->min_args;
        for(i = 0; c->cmd[i]; i++){
            ucase[i] = toupper(c->cmd[i]);
        }
        c_hU->cmd = ucase;
        HASH_ADD_KEYPTR(hh, kvolve_commands, c_hU->cmd, strlen(c_hU->cmd), c_hU);
    }
}

kvolve_call kvolve_lookup_kv_command(redisClient * c){

    struct kvolve_cmd_hash * c_h = NULL;
    char * lookup = (char*)c->argv[0]->ptr;
    if(!kvolve_commands)
        kvolve_populateCommandTable();
    HASH_FIND(hh, kvolve_commands, lookup, strlen(lookup), c_h);
    
    if(!c_h || (c->argc < c_h->min_args))
        return NULL;
    return c_h->call;
}

#define __GNUC__  // "re-unallow" malloc
