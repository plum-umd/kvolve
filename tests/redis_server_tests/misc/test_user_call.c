#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "kvolve_upd.h"


void test_fun_1(char ** key, void ** value, size_t * val_len){
    printf("ORDER CALLEDDDDDDDDDD\n");
    kvolve_upd_redis_call("set order:222 wwwww");
}

void kvolve_declare_update(){
    kvolve_upd_spec("order", "order", 5, 6, 1, test_fun_1);
}

