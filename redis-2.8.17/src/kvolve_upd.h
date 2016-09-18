#ifndef _KVOLVE_UPD_H
#define _KVOLVE_UPD_H

/*****************************************************
 Connecting to Redis-KVolve:

    Connect to Redis as normal, and then immediately after connecting call
       client setname [namespace]@[version],...
       Ex: client setname order@v0,user@u0

*****************************************************/


/*****************************************************
 Writing the Update Function:

    To write the update, the update-writer uses one function call to
      describe each update (version info), with optional parameters of update
      functions to perform along with updating the version info.

    The specifics are described below, but here is a brief example of appending
      a string "UPDATED" to all values of keys in the namespace "order":

      -------------------------------------------------------------------
      void test_fun_updval(char ** key, void ** value, size_t * val_len){  // The update function prototype
          size_t s = strlen("UPDATED")+strlen((char*)*value)+1;
          char * cons = calloc(s,sizeof(char));
          strcat(cons, *value);
          strcat(cons, "UPDATED");
          *value = cons;   // Set the updated value
          *val_len = s;    // Set the updated value length (necessary to allow binary strings)
      }

      void kvolve_declare_update(){  // Place your update calls here
          kvolve_upd_spec("order", "order", 0, 1, 1, test_fun_updval);  // This describes the update
      }
      -------------------------------------------------------------------
*****************************************************/

/* This is the typedef for the function prototype that the user must write.
 *    Ex: void test_fun_1(char ** key, void ** value, size_t * val_len){...}
 *
 *    Important: No user-written function is necessary to update namespaces unless
 *           additional modifications are necessary. The @key parameter allows the
 *           user to do some additional changes to a specific key if desired.
 *
 *    @key : A modifiable pointer to the key.  If the user specifies a namespace
 *           change in the update info, that namespace change will be applied automatically.
 *           In case of a namespace change, @key will be the already-modified key.
 *
 *           If a change to the key is desired, set "*key" in this function.
 *
 *    @value : A modifiable pointer to the data.  If a change to the value is
 *           desired, set "*value" in this function.
 *
 *    @val_len : The modifiable length of the data. This is necessary because not all @value
 *           will be null-terminated (Ex: binary file data).  If a change to the value is
 *           performed, this length must be updated by setting *val_len to the new length.
 */
typedef void (*kvolve_upd_fun)(char ** key, void ** value, size_t * val_len);


/* This function specifies an update.  There must be 1 more calls to this function per update.
 *    Ex: kvolve_upd_spec("order", "order", 0, 1, 1, upd_fun_name);
 *        kvolve_upd_spec("user", "user:region", 0, 1, 0);
 *
 *    @from_ns : The namespace we're updating from. This must have already been
 *           declared by a connecting program.
 *    @to_ns : The namespace we're updating to. This will be equal to @from_ns unless
 *           there is a namespace change
 *    @from_vers : The version we're updating from. This must have already been
 *           declared by a connecting program.
 *    @to_vers : The version we're updating to.  These must be uniquely named with a namespace.
 *    @n_funs : The number of functions of type kvolve_upd_fun for this update
 *    @... : Zero or more function pointers to type kvolve_upd_fun.
 */
void kvolve_upd_spec(char *from_ns, char * to_ns, int from_vers, int to_vers, int n_funs, ...);


/* This is the function where to place the calls to kvolve_upd_spec.  This will
 * load up all of the update information automatically */
void kvolve_declare_update() __attribute__((constructor));


/* This is an optional helper call that allows the update-writer to call redis
 * in the update function.
 *    Ex: kvolve_upd_redis_call("set order:222 wwwww");
 * This returns the server response as a string following the usual redis protocol, which
 * may be either ignored (if a set) or parsed with hiredis redisReaderFeed.
 */
char * kvolve_upd_redis_call(char * userinput);


/* Structures for the **value parameter for type REDIS_HASH and type REDIS_ZSET */
struct hash_subkeyval{
    char * hashkey; 
    char * hashval;
};
struct zset_scoreval{
    char * setelem;
    double * score;
};


/*****************************************************
 Compiling the Update Module:

    Compile the above function to a shared object by adding these flags:
       -shared -fPIC -g your_upd_fun_file.c -o your_upd_name.so -ldl -I../redis-2.8.17/src/

    (Make sure to change the correct location of the includes, or install)
*****************************************************/


/*****************************************************
 Calling the Update Module:

    Updates may be requested within the program or outside the program by issuing
    'client setname update/' concatenated with the path to your module.
       Ex: client setname update/path_to_your_module/your_upd_name.so

    This will automatically advance the namespace for the calling client, all
       other programs connected to the updated namespace must update.

*****************************************************/

#endif
