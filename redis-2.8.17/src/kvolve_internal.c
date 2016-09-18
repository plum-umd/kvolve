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
#include <stdarg.h>
#undef __GNUC__  // allow malloc (needed for uthash)  (see redis.h ln 1403)
#include "uthash.h"
#include "kvolve_internal.h"
#include "redis.h"
#include "kvolve_upd.h"
#include "kvolve.h"

extern int processInlineBuffer(redisClient *c);
extern double zzlGetScore(unsigned char *sptr);
/* The hash handle for all of the namespace versions*/
static struct version_hash * vers_list = NULL;
/* The hash handle for temporary version storage */
struct tmp_vers_store_hash * tmp_store = NULL;
/* The initial size of the version arrays in the hashtable*/
#define KV_INIT_SZ 20

/* client for the user's mu code calls (stored to free) */
redisClient * c_fake_user = NULL; 

/* this flag indicates that an update function is being processed.  (Prevents
 * recursion in case of user making calls during update function*/
int upd_fun_running = 0;
uint64_t loading_id = 0;

struct version_hash * kvolve_create_ns(redisClient *c, char *ns_lookup, char *prev_ns, int v0, struct kvolve_upd_info * list){
    struct version_hash *tmp, * v = (struct version_hash*)malloc(sizeof(struct version_hash));
    int i;
    v->ns = malloc(strlen(ns_lookup)+1);
    strcpy(v->ns, ns_lookup); 
    v->prev_ns = NULL;
    v->next_ns = NULL;
    if (prev_ns){
        v->prev_ns = malloc(strlen(prev_ns)+1);
        strcpy(v->prev_ns, prev_ns); 
        HASH_FIND(hh, vers_list, prev_ns, strlen(prev_ns), tmp);
        v->num_versions = 1+ tmp->num_versions;
        v->versions = calloc(KV_INIT_SZ,sizeof(int)); //TODO check resize
        v->info = calloc(KV_INIT_SZ,sizeof(struct kvolve_upd_info*));
        for(i = 0; i< tmp->num_versions; i++){
            v->versions[i] = tmp->versions[i];
            v->info[i] = tmp->info[i];
        }
        v->versions[tmp->num_versions] = v0;
        v->info[tmp->num_versions] = list;
        tmp->next_ns = v->ns;
    } else {
        v->num_versions = 1;
        v->versions = calloc(KV_INIT_SZ,sizeof(int));
        v->versions[0] = v0;
        v->info = calloc(KV_INIT_SZ,sizeof(struct kvolve_upd_info*));
        v->info[0] = NULL;
    }
    v->is = intsetNew();
    if(c)
        v->is = intsetAdd(v->is,c->id,NULL);
    else if (loading_id)
        v->is = intsetAdd(v->is,loading_id,NULL);
    else{
        DEBUG_PRINT(("No connecting ID set for %s", v->ns));
    }
    HASH_ADD_KEYPTR(hh, vers_list, v->ns, strlen(v->ns), v);
    return v;
}


/* Get the keyname from @orig_key and combine it with @old_ns.  
 * Allocates memory for the new string and returns it. */
char * kvolve_construct_prev_name(char * orig_key, char *old_ns){
    char * name = strrchr(orig_key, ':');
    char * ret = malloc(strlen(name)+strlen(old_ns) +1);
    strcpy(ret, old_ns);
    strcat(ret, name);
    return ret;
}

void kvolve_checkdel_old(redisClient * c, struct version_hash * v){

    struct version_hash * tmp = v;
    robj * key, * val;
    if(v == NULL) return;

    /* Iterate prev namespaces */
    while(tmp && tmp->prev_ns){
        char * old = kvolve_construct_prev_name((char*)c->argv[1]->ptr, tmp->prev_ns);
        DEBUG_PRINT(("creating with old = %s\n", old));
        key = createStringObject(old,strlen(old));
        free(old);
        val = lookupKey(c->db, key);
        if (val || !tmp->prev_ns)
            break;
        zfree(key);
        key = NULL;
        tmp = kvolve_version_hash_lookup(tmp->prev_ns);
    }
    if(key){
        dbDelete(c->db, key);
        zfree(key);
    }
}

void kvolve_check_version(redisClient *c){

    char *  vers_str = (char*)c->argv[2]->ptr;
    if(!strrchr(vers_str, '@')) return;
    int toprocess =  strlen(vers_str);
    char * cpy = malloc(strlen(vers_str)+1);
    strcpy(cpy, vers_str);

    

    while(1) {
        char * ns_lookup; 
        char * vers;
        int vers_i;
        if (strcmp(cpy, vers_str) == 0)
            ns_lookup = strtok(cpy, "@");
        else
            ns_lookup = strtok(NULL, "@"); /* We've already started processing */
        vers = strtok(NULL, ",");
        vers_i = atoi(vers);
        int pos = strlen(vers);
  
        struct version_hash *v = NULL;
  
        HASH_FIND(hh, vers_list, ns_lookup, strlen(ns_lookup), v);
        if (v==NULL){
            v = kvolve_create_ns(c, ns_lookup, NULL, vers_i, NULL);
        } else if (v->versions[v->num_versions-1] != vers_i){
            printf("ERROR, INVALID VERSION (%d). System is at \'%d\' for ns \'%s\'\n", 
                   vers_i, v->versions[v->num_versions-1], v->ns);
            sdsfree(c->argv[0]->ptr);
            /* This will close the connection */
            c->argv[0]->ptr = sdsnew("quit");
            return;
        } else if (v->next_ns){
            printf("ERROR, NAMESPACE (%s) DEPRECATED TO (%s).\n", v->ns, v->next_ns);
            c->argv[0]->ptr = sdsnew("quit");
            return;
        } else { 
            v->is = intsetAdd(v->is,c->id,NULL);
        }
        if (&vers[pos] == &cpy[toprocess])
            return;
    }
}

int kvolve_get_flags(redisClient *c){
    int flags = REDIS_SET_NO_FLAGS;
    int j;

    for (j = 3; j < c->argc; j++) {
        char *a = c->argv[j]->ptr;

        if ((a[0] == 'n' || a[0] == 'N') &&
            (a[1] == 'x' || a[1] == 'X') && a[2] == '\0') {
            flags |= REDIS_SET_NX;
        } else if ((a[0] == 'x' || a[0] == 'X') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0') {
            flags |= REDIS_SET_XX;
        }

    }
    return flags;
}

void kvolve_load_update(redisClient *c, char * upd_code){

    void *handle;
    char *errstr;
    int err;
    struct stat s;
    loading_id = c->id;
  
    DEBUG_PRINT(("Updating with %s\n", upd_code));
    err = stat(upd_code, &s);
    if(-1 == err) {
        printf("ERROR, update file %s does not exist.\n", upd_code);
        loading_id = 0;
        return;
    }

    /* The GLOBAL flag for 3rd party libraries.  The DEEPBIND flag so that
     * previous versions of 'kvolve_upd_spec' are replaced with the one about to be
     * loaded.*/
    handle = dlopen(upd_code, RTLD_NOW | RTLD_GLOBAL | RTLD_DEEPBIND);
    if (handle == NULL){
        errstr = dlerror();
        loading_id = 0;
        printf ("A dynamic linking error occurred: (%s)\n", errstr);
        return;
    }
    loading_id = 0;
}

/* This API function allows the update-writer to call into redis from the
 * update function (mu). */
char * kvolve_upd_redis_call(char* userinput){

    /* free from prev if necessary*/
    if(c_fake_user)
        freeClient(c_fake_user);
    c_fake_user = createClient(-1);
    size_t buff = strlen(userinput)+3;
    char * q = malloc(buff);
    /* add redis protocol fun */
    sprintf(q,"%s\r\n",userinput);
    c_fake_user->querybuf = sdsnew(q);
    free(q);
    /* parse the user input string */
    processInlineBuffer(c_fake_user);
    /* lookup the newly parsed command */
    c_fake_user->cmd = lookupCommandOrOriginal(c_fake_user->argv[0]->ptr);
    /* run through kvolve (set vers, and set flag to not run updates on this
     * value, else infinite loop!), then call properly*/
    kvolve_process_command(c_fake_user);
    call(c_fake_user, 0);
    return c_fake_user->buf;
}

/* This is the API function that the update-writer calls to load the updates */
void kvolve_upd_spec(char *from_ns, char * to_ns, int from_vers, int to_vers, int n_funs, ...){

    int i, created = 0;
    struct version_hash * v, *tmp, *tmp2;
    struct kvolve_upd_info * info;
    va_list arguments;
    uint64_t k;
    char * kill = "client kill id ";  //TODO rewrite this section
    char q[24];

    /* Initializing arguments to store all values after num */
    va_start(arguments, n_funs);

    HASH_FIND(hh, vers_list, from_ns, strlen(from_ns), v);
    /* make sure namespace exists */
    if (v == NULL){
        printf("No such namespace (%s) to upgrade.\n", from_ns);
        return;
    } else if (strcmp(from_ns, to_ns) != 0){
        /* check if namespace exists already */
        HASH_FIND(hh, vers_list, to_ns, strlen(to_ns), v);
        if (v != NULL){
            printf("Cannot merge into existing ns (%s) from ns (%s).\n",
                   to_ns, from_ns);
            return;
        }
    } else {
        /* make sure update was not already loaded */
        for (i = 0; i < v->num_versions; i++){
            if (v->versions[i] == to_vers){
                printf("ERROR, previous version %s already loaded...\n", to_ns);
                return;
            }
        }
    }
    /* If not a new ns, make sure the previous version exists */
    if (v && v->prev_ns == NULL && (v->versions[v->num_versions-1] != from_vers)){
        printf("No such version (%d) to upgrade for ns (%s).\n",
               from_vers, from_ns);
        return;
    }

    /* If we've made it this far, create the info stucture */
    info = malloc(sizeof(struct kvolve_upd_info));
    info->from_ns = from_ns;
    info->to_ns = to_ns;
    info->from_vers = from_vers;
    info->to_vers = to_vers;
    info->num_funs = n_funs;
    info->funs = calloc(n_funs, sizeof(kvolve_upd_fun));
    for (i = 0; i<n_funs; i++){
        info->funs[i] = va_arg(arguments, kvolve_upd_fun);
    }
    /* If v is null, we need a new namespace */
    if (!v){
        v = kvolve_create_ns(NULL, to_ns, from_ns, to_vers, info);
        created = 1;
    }
     
    /* This loop will kill off deprecated clients (including ones from the old namespace if 
     * we just created a new one)*/
    tmp = v;
    while(tmp){
       while(tmp->is->length > 1){ /* 1 because we don't kill caller */
           k = intsetRandom(tmp->is);
           if(k == loading_id) /* don't kill caller */
               continue;
           tmp->is = intsetRemove(tmp->is, k, NULL);
           DEBUG_PRINT(("KILLING CLIENT WITH ID %ld\n", k));
           sprintf(q,"%s%ld\r\n", kill, k);
           kvolve_upd_redis_call(q);
       }
       if (tmp->prev_ns){
           HASH_FIND(hh, vers_list, tmp->prev_ns, strlen(tmp->prev_ns), tmp2);
           tmp = tmp2;
       }
       else
           break;
    }
    if(created == 1) /* the below will already be set. */
        return;

    v->is = intsetAdd(v->is, loading_id, NULL);
    if (v->num_versions > KV_INIT_SZ){ /*TODO change this when resize impl'ed */
        /* TODO, dynamically resize array */
        printf("CANNOT APPEND, REALLOC NOT IMPLEMENTED, TOO MANY VERSIONS.\n");
        return;
    }
    v->versions[v->num_versions] = to_vers;
    v->info[v->num_versions] = info;
    v->num_versions++;
 
}

/* Looks for a prepended namespace in @lookup (longest matching prefix), and
 * then lookups and returns the version information in the hashtable if it
 * exists, else returns null.  */
struct version_hash * kvolve_version_hash_lookup(char * lookup){
    struct version_hash *v = NULL;
    size_t len;

    if (!vers_list) return NULL;
    /* Split out the namespace from the key, if a namespace exists. */
    char * split = strrchr(lookup, ':');
    if (split == NULL){
        HASH_FIND(hh, vers_list, "*", 1, v);/* check for global default namespace */
        if (!v){
            DEBUG_PRINT(("WARNING: No namespace declared for key %s\n", lookup));
        }
        return v;
    }
    len = split - lookup + 1;

    /* Get the current version for the namespace, if it exists */
    HASH_FIND(hh, vers_list, lookup, len-1, v);

    /* If not found, recurse search for next longest prefix */
    if(!v){
        lookup[len-1] = '\0';
        if(strrchr(lookup, ':'))
            v = kvolve_version_hash_lookup(lookup);
        lookup[len-1]=':';
    }
    return v;
}


/* return the VALUE with the namespace that's currently in the db */
robj * kvolve_get_db_val(redisClient * c, struct version_hash * v){

    struct version_hash * tmp = v;
    robj * key, * val;
    if(v == NULL) return NULL;

    /* first check the obvious (current) */
    val = lookupKey(c->db, c->argv[1]);
    if(val) return val;

    /* Iterate prev namespaces */
    while(tmp && tmp->prev_ns){
        char * old = kvolve_construct_prev_name((char*)c->argv[1]->ptr, tmp->prev_ns);
        DEBUG_PRINT(("creating with old = %s\n", old));
        key = createStringObject(old,strlen(old));
        free(old);
        val = lookupKey(c->db, key);
        zfree(key);
        if (val) return val;
        if(!tmp->prev_ns)
            break;
        tmp = kvolve_version_hash_lookup(tmp->prev_ns);
    }
    return NULL;
}


void kvolve_namespace_update(redisClient * c, struct version_hash * v) {

    if(lookupKey(c->db, c->argv[1]))
        return;
    redisClient * c_fake = createClient(-1);
    c_fake->argc = 3;
    c_fake->argv = zmalloc(sizeof(void*)*3);
    char * old = kvolve_construct_prev_name((char*)c->argv[1]->ptr, v->prev_ns);
    c_fake->argv[1] = createStringObject(old,strlen(old));
    c_fake->argv[2] = c->argv[1]; 
    sds ren = sdsnew("rename");
    c_fake->cmd = lookupCommand(ren);
    c_fake->cmd->proc(c_fake);
    DEBUG_PRINT(("Updated key (namespace) from %s to %s\n", 
                 old, (char*)c_fake->argv[2]->ptr));

    zfree(c_fake->argv[1]);
    zfree(c_fake->argv);
    zfree(c_fake);
    sdsfree(ren);
    free(old);
}


/* checks if rename is necessary then performs it (for nargs args) .*/
void kvolve_check_rename(redisClient * c,struct version_hash * v, int nargs){

    int i;
    robj * o;

    /* return immediately if there is no chance of ns change */
    if(!v || !v->prev_ns)
        return;

    redisClient * c_fake = createClient(-1);
    c_fake->db = c->db;
    c_fake->argc = 2;
    c_fake->argv = zmalloc(sizeof(void*)*2);

    for (i=1; i < nargs; i++){
        c_fake->argv[1]= c->argv[i];
        o = kvolve_get_db_val(c_fake, v);
        if (!o)
            continue;

        // strings stored as ints don't have vers. Check for rename manually.
        if (o->type == REDIS_STRING && o->encoding == REDIS_ENCODING_INT){
            kvolve_namespace_update(c_fake, v);
        } else if(o->vers != v->versions[v->num_versions-1]){
            kvolve_namespace_update(c_fake, v);
        }
    }
    zfree(c_fake->argv);
    zfree(c_fake);
}

/* THIS IS THE UPDATE FUNCTION. See header for documentation */
void kvolve_check_update_kv_pair(redisClient * c, struct version_hash * v, int check_key, robj * o, int type, double * s, robj * fval){

    int i, key_vers = -1, fun;
    struct hash_subkeyval hsk;
    struct zset_scoreval zsv;
    if(v == NULL) return;

    /* Make sure that we're not here because of user update code (kvolve_user_call)*/
    if (upd_fun_running == 1)
        return;

    /* If the object wasn't passed in (string type),
     * then look it up (as a robj with version info) */
    if (!o){
        o = kvolve_get_db_val(c, v);
        if (!o) return;
    }

    /* String types that have integer values don't have versions because redis
     * uses shared.integer[val] objects to encode these.  (Because the values may
     * be shared by multiple keys, there's no safe way to store the version as they
     * may not be consistent.) However, the keys could still be renamed, check for
     * this, then return. */
    if (o->encoding == REDIS_ENCODING_INT && o->type == REDIS_STRING
        && (((long long)o->ptr) < REDIS_SHARED_INTEGERS)){
        kvolve_check_rename(c, v, 2);
        return;
    }

    /* Check to see if the version is current, if so, return. */
    if (o->vers == v->versions[v->num_versions-1])
        return;

    /* Key is present at an older version. Time to update, get version. */
    upd_fun_running = 1;
    for (i = 0; i < v->num_versions; i++){
        if (o->vers == v->versions[i]){
            key_vers = i;
            break;
        }
    }
    
    /* check if we need to rename the key based on a new namespace */
    if(check_key && v->info[key_vers+1] && 
                 (strcmp(v->info[key_vers+1]->from_ns, v->ns)!=0)){
        kvolve_namespace_update(c, v);
    }

    /* call all update functions */
    for ( ; key_vers < v->num_versions-1; key_vers++){
        /* in some cases, there may be multiple updates */
        if (!v->info[key_vers+1]){
            DEBUG_PRINT(("Warning: no update functions for %s:%d\n", 
                   v->ns, v->versions[key_vers+1]));
            o->vers = v->versions[key_vers+1];
            continue;
        }
        for (fun=0; fun < v->info[key_vers+1]->num_funs; fun++){
            char * key = (char*)c->argv[1]->ptr;
            char * val = (char*)o->ptr;
            size_t val_len = 0;
            if(o->encoding == 0)
                val_len = sdslen((sds)o->ptr);
            /* next lines calls the update func (mods key/val as specified): */
            if(type == REDIS_ZSET){
                zsv.setelem = val;
                zsv.score = s;
                v->info[key_vers+1]->funs[fun](&key, (void*)&zsv, &val_len);
            } else if(type == REDIS_HASH){
                hsk.hashkey = o->ptr;
                hsk.hashval = fval->ptr;
                v->info[key_vers+1]->funs[fun](&key, (void*)&hsk, &val_len);
            } else {
                v->info[key_vers+1]->funs[fun](&key, (void*)&val, &val_len);
            }
            /* Now check to see what,if anything, was modified by the user code*/
            /* keys */
            if (check_key && (key != (char*)c->argv[1]->ptr)){
                /* The key will automatically be renamed if a namespace change
                 * is specified in 'struct version_hash'.  However, this gives the user a
                 * chance to do some further custom modifications if necessary. */ 
                DEBUG_PRINT(("Updated key (custom) from %s to %s\n", 
                             (char*)c->argv[1]->ptr, key));
                sdsfree(c->argv[1]->ptr); // free old memory
                c->argv[1]->ptr = sdsnew(key); // memcpy's key (user alloc'ed)
                free(key); // free user-update-allocated memory
                /* This will write the user-modified key to disk */
                kvolve_namespace_update(c, v);
            }
            /* vals */
            if (type == REDIS_STRING && val != (char*)o->ptr){
                DEBUG_PRINT(("Updated str value from %s to %s\n", (char*)o->ptr, val));
                sdsfree(o->ptr);
                o->ptr = sdsnewlen(val, val_len);
                free(val);
                /* This will notify any client watching key (normally called 
                 * automatically, but we bypassed by changing val directly */
                signalModifiedKey(c->db,o);
                server.dirty++;
            } else if (type == REDIS_SET && val != (char*)o->ptr){
                DEBUG_PRINT(("Updated set value from %s to %s\n", (char*)o->ptr, val));
                kvolve_update_set_elem(c, val, &o);
            } else if (type == REDIS_ZSET && (zsv.setelem != o->ptr || zsv.score != s)){
                kvolve_update_zset_elem(c, zsv.setelem, &o, *zsv.score);
            } else if (type == REDIS_HASH && (hsk.hashkey != o->ptr || hsk.hashval != fval->ptr)){
                kvolve_update_hash_elem(c, hsk.hashkey, hsk.hashval, &o);
            } else if (type == REDIS_LIST && val != (char*)o->ptr){
                DEBUG_PRINT(("Updated list value from %s to %s\n", (char*)o->ptr, val));
                kvolve_update_list_elem(c, val, &o);
            }
        }
        /* Update the version string in the key to match the update we just did.*/
        o->vers = v->versions[key_vers+1];
    }
    upd_fun_running = 0;
}

void kvolve_update_set_elem(redisClient * c, char * new_val, robj ** o){

    sds cmd = NULL;
    robj * new;
    /* add new */
    redisClient * c_fake = createClient(-1);
    c_fake->db = c->db;
    c_fake->argc = 3;
    c_fake->argv = zmalloc(sizeof(void*)*3);
    c_fake->argv[1] = c->argv[1];
    new = createStringObject(new_val,strlen(new_val));
    c_fake->argv[2] = new;
    cmd = sdsnew("sadd");
    c_fake->cmd = lookupCommand(cmd);
    sdsfree(cmd);
    c_fake->cmd->proc(c_fake);

    /* delete old */
    c_fake->argv[2] = *o;
    cmd = sdsnew("srem");
    c_fake->cmd = lookupCommand(cmd);
    sdsfree(cmd);
    c_fake->cmd->proc(c_fake);

    zfree(c_fake->argv);
    zfree(c_fake);
    *o = new;
}

void kvolve_update_hash_elem(redisClient * c, char * new_skval, char * new_fval, robj ** o){
    sds cmd = NULL;
    robj * new_skval_o, *new_fval_o;


    /* add new */
    redisClient * c_fake = createClient(-1);
    c_fake->db = c->db;
    c_fake->argc = 4;
    c_fake->argv = zmalloc(sizeof(void*)*4);
    c_fake->argv[1] = c->argv[1];
    new_skval_o = createStringObject(new_skval,strlen(new_skval));
    c_fake->argv[2] = new_skval_o;
    new_fval_o = createStringObject(new_fval,strlen(new_fval));
    c_fake->argv[3] = new_fval_o;
    cmd = sdsnew("hset");
    c_fake->cmd = lookupCommand(cmd);
    sdsfree(cmd);
    c_fake->cmd->proc(c_fake);
    DEBUG_PRINT(("Updated hash subkey %s to have value %s\n", (char*)new_skval, new_fval));

    /* delete old */
    if(strcmp((char*)(*o)->ptr, (char*)new_skval_o->ptr)!=0){
        c_fake->argc = 3;
        c_fake->argv[2] = *o;
        cmd = sdsnew("hdel");
        c_fake->cmd = lookupCommand(cmd);
        sdsfree(cmd);
        c_fake->cmd->proc(c_fake);
        DEBUG_PRINT(("Updated hash subkey NAME from %s to %s\n", (char*)(*o)->ptr, new_skval));
    }

    zfree(c_fake->argv);
    zfree(c_fake);
    *o = new_skval_o;
}

void kvolve_update_list_elem(redisClient * c, char * new_val, robj ** o){

    sds cmd = NULL;
    robj * new;
    /* add new */
    redisClient * c_fake = createClient(-1);
    c_fake->db = c->db;
    c_fake->argc = 5;
    c_fake->argv = zmalloc(sizeof(void*)*5);
    c_fake->argv[1] = c->argv[1];
    new = createStringObject(new_val,strlen(new_val));
    c_fake->argv[2] = createStringObject("BEFORE",strlen("BEFORE"));
    c_fake->argv[3] = *o;
    c_fake->argv[4] = new;
    cmd = sdsnew("linsert");
    c_fake->cmd = lookupCommand(cmd);
    sdsfree(cmd);
    c_fake->cmd->proc(c_fake);
    zfree(c_fake->argv[2]);

    /* delete old */

    c_fake->argc = 4;
    c_fake->argv[2] = createStringObjectFromLongLong(1);
    c_fake->argv[3] = *o;
    cmd = sdsnew("lrem");
    c_fake->cmd = lookupCommand(cmd);
    sdsfree(cmd);
    c_fake->cmd->proc(c_fake);

    zfree(c_fake->argv);
    zfree(c_fake);
    *o = new;
}

void kvolve_update_zset_elem(redisClient * c, char * new_val, robj ** o, double s){

    sds ren = NULL;
    robj * new, *scoreobj;
    char output[50];

    snprintf(output,50,"%f",s);

    /* add new */
    redisClient * c_fake = createClient(-1);
    c_fake->db = c->db;
    c_fake->argc = 4;
    c_fake->argv = zmalloc(sizeof(void*)*3);
    c_fake->argv[1] = c->argv[1];
    new = createStringObject(new_val,strlen(new_val));
    scoreobj = createStringObject(output,strlen(output));
    c_fake->argv[2] = scoreobj;
    c_fake->argv[3] = new;
    ren = sdsnew("zadd");
    c_fake->cmd = lookupCommand(ren);
    sdsfree(ren);
    c_fake->cmd->proc(c_fake);
    DEBUG_PRINT(("Updated zset value %s to have score %f\n", new_val, s));

    /* delete old if necesary */
    if(strcmp((char*)(*o)->ptr, new_val) != 0 ){
        DEBUG_PRINT(("Updated zset value element from %s to %s\n", (char*)(*o)->ptr, new_val));
        c_fake->argv[2] = *o;
        c_fake->argc = 3;
        (*o)->encoding = REDIS_ENCODING_RAW;
        ren = sdsnew("zrem");
        c_fake->cmd = lookupCommand(ren);
        sdsfree(ren);
        c_fake->cmd->proc(c_fake);
    }

    zfree(c_fake->argv);
    zfree(c_fake);
    zfree(scoreobj);
    *o = new; //TODO check freeing
}

/* Make sure all set elements are at this current version and update them
 * all if necessary.  Don't let different members of the same set be at different
 * versions!! (would be a confusing mess.) This will check and return if current,
 * else update other set members to the current version. This will also update the
 * namespace of the key if necessary. */
int kvolve_update_all_set(redisClient * c, struct version_hash * v){
    int first = 1, set_len, i=0;
    robj ** robj_array;
    
    if(v == NULL)
        return 0;
    robj * o = kvolve_get_db_val(c, v);
    if (o == NULL) 
        return 1;
    /* return if object is already current */
    if (o->vers == v->versions[v->num_versions-1])
        return 0;

    setTypeIterator *si = setTypeInitIterator(o);
    robj * e = setTypeNextObject(si);
    set_len = setTypeSize(o);
    robj_array = calloc(set_len, sizeof(robj*));

    /* get the set elements w/o modifying the set */
    while(e){
        robj_array[i] = e;
        i++;
        e = setTypeNextObject(si);
    }

    /* call update (modify) on each of the set elements */
    for(i=0; i<set_len; i++){
        robj_array[i]->vers = o->vers;
        kvolve_check_update_kv_pair(c, v, first, robj_array[i], REDIS_SET, NULL, NULL);
        /* namespace change checked, no need to repeat */
        first = 0;
    }
    setTypeReleaseIterator(si);
    free(robj_array);

    /* Update the version string in the set container to match the update we
     * just did on the set members .*/
    o->vers = v->versions[v->num_versions-1];

    return 0;
}

int kvolve_update_all_zset(redisClient * c, struct version_hash * v){

    unsigned char *vstr;
    unsigned int vlen;
    long long vll;
    robj * elem;
    int first = 1;
    int i = 0;
    int zset_len;
    double * score_array;
    robj ** robj_array;
    unsigned char *p;
    robj * o;

    if(v == NULL) return 0;
    o = kvolve_get_db_val(c, v);
    if (o == NULL)
        return 1;
    /* return if object is already current */
    if(o->vers == v->versions[v->num_versions-1])
        return 0;
    if(o->encoding != REDIS_ENCODING_ZIPLIST){ //TODO REDIS_ENCODING_SKIPLIST
        DEBUG_PRINT(("Type %d not implemented for zset\n", o->encoding));
        return 0;
    }

    zset_len = zsetLength(o);
    score_array = calloc(zset_len, sizeof(double));
    robj_array = calloc(zset_len, sizeof(robj*));
    p = ziplistIndex(o->ptr,0);


    /* iterate over the zset and get the objects/scores */
    while(p) { //db.c:515
        ziplistGet(p,&vstr,&vlen,&vll);
        if(vstr){
            elem = createStringObject((char*)vstr,vlen);
            elem->vers = o->vers;
            elem->type = o->type;
            elem->encoding = o->encoding;
            p = ziplistNext(o->ptr,p);
            ziplistGet(p,&vstr,&vlen,&vll);
            score_array[i] = zzlGetScore(p);
            robj_array[i] = elem;
            i++;
        }
        p = ziplistNext(o->ptr,p);
    }
    /* now modify the zset */
    for(i=0; i<zset_len; i++){
        kvolve_check_update_kv_pair(c, v, first, robj_array[i], o->type, &score_array[i], NULL);
        zfree(robj_array[i]);
        first = 0;
    }
    free(score_array);
    free(robj_array);

    o->vers = v->versions[v->num_versions-1];
    return 0;
}

int kvolve_update_all_hash(redisClient * c, struct version_hash * v){
    int first = 1, length, i=0;
    if(v == NULL)
        return 0;
    robj * o = kvolve_get_db_val(c, v);
    if (o == NULL) 
        return 1;
    /* return if object is already current */
    if (o->vers == v->versions[v->num_versions-1])
        return 0;
    length = hashTypeLength(o) * 2;

    robj ** robj_array = calloc(length, sizeof(robj*));
    hashTypeIterator *hi = hashTypeInitIterator(o);
    /* gather the (hashsubkey, hashfieldval) elements */
    while(hashTypeNext(hi) == REDIS_OK){
        robj_array[i] = hashTypeCurrentObject(hi, REDIS_HASH_KEY);
        robj_array[i+1] = hashTypeCurrentObject(hi, REDIS_HASH_VALUE);
        i=i+2;
    }

    /* now call update on each pair */
    for(i=0; i<length; i=i+2){
        robj_array[i]->vers = o->vers;
        kvolve_check_update_kv_pair(c, v, first, robj_array[i], REDIS_HASH, NULL, robj_array[i+1]);
        zfree(robj_array[i]);
        zfree(robj_array[i+1]);
        /* namespace change checked, no need to repeat */
        first = 0;
    }

    /* Update the version string in the set container to match the update we
     * just did on the set members .*/
    o->vers = v->versions[v->num_versions-1];
    free(robj_array);
    hashTypeReleaseIterator(hi);

    return 0;
}

/* Check if update is necessary for a list and if so do it. */
int kvolve_update_all_list(redisClient * c, struct version_hash * v){
    if(v == NULL)
        return 0;
    int first = 1, exists = 0, i=0;
    robj *e, * o = kvolve_get_db_val(c, v);
    if (o == NULL)
        return 1;
    listTypeEntry entry;
    /* return if object is already current */
    if (o->vers == v->versions[v->num_versions-1])
        return 0;
    int list_len = listTypeLength(o);
    robj ** robj_array = calloc(list_len, sizeof(robj*));


    /* iterate over the list and get the items (don't modify yet) */
    listTypeIterator *li = listTypeInitIterator(o, 0, REDIS_TAIL);
    exists = listTypeNext(li, &entry);
    while(exists){
        e = listTypeGet(&entry);
        exists = listTypeNext(li, &entry);
        robj_array[i] = e;
        i++;
        /* namespace change checked, no need to repeat */
    }

    /* now modify the list */
    for(i=0; i < list_len; i++){
        e = robj_array[i];
        e->vers = o->vers;
        kvolve_check_update_kv_pair(c, v, first, e, REDIS_LIST, NULL, NULL);
        zfree(robj_array[i]);
        first = 0; /* only check for key namespace change once */
    }
    free(robj_array);
    listTypeReleaseIterator(li);

    /* Update the version string in the set container to match the update we
     * just did on the set members .*/
    o->vers = v->versions[v->num_versions-1];

    return 0;
}

void kvolve_prevcall_check(void){

    struct tmp_vers_store_hash *current_fix, *tmp;
    robj * o;
    dictEntry *de;
    sds key;
    int remains = 0;

    if(!tmp_store) return;

    HASH_ITER(hh, tmp_store, current_fix, tmp) {
        key = sdsnew(current_fix->key);
        de = dictFind(current_fix->prev_db->dict,key);
        sdsfree(key);
        if(!de){ /* happens if multi block */
            remains = 1;
            continue;
        }
        o = dictGetVal(de);
        o->vers = current_fix->vers;

        HASH_DEL(tmp_store,current_fix);
        free(current_fix->key);
        free(current_fix);
    }
    if(!remains)
        tmp_store = NULL;
}

void kvolve_new_version(redisClient *c, struct version_hash * v){
    struct tmp_vers_store_hash * t;
    char * tmp_name;
    HASH_FIND(hh, tmp_store, (char*)c->argv[1]->ptr, strlen((char*)c->argv[1]->ptr), t);
    if (t){
        t->vers = v->versions[v->num_versions-1];
        t->prev_db = c->db;
        return;
    }
    t = malloc(sizeof(struct tmp_vers_store_hash));
    tmp_name = malloc(strlen((char*)c->argv[1]->ptr)+1);
    strcpy(tmp_name, (char*)c->argv[1]->ptr);
    t->key = tmp_name;
    t->vers = v->versions[v->num_versions-1];
    t->prev_db = c->db;
    HASH_ADD_KEYPTR(hh, tmp_store, t->key, strlen(t->key), t);
}

#define __GNUC__  // "re-unallow" malloc

