#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "kvolve_upd.h"

/* test with key change */

void test_fun_ns_change(char ** key, void ** value, size_t * val_len){
    /* No need to do anything here unless additional mods are necessary.
     * Namespace will update automatically*/
}

void kvolve_declare_update(){
    kvolve_upd_spec("order", "foo:order", 6, 70, 1, test_fun_ns_change);
}

