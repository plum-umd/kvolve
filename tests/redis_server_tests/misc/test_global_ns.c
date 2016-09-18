#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "kvolve_upd.h"


void test_fun_1(char ** key, void ** value, size_t * val_len){
    printf("ORDER CALLEDDDDDDDDDD\n");
    size_t s = strlen("UPDATED")+strlen((char*)*value)+1;
    char * cons = calloc(s,sizeof(char));
    strcat(cons, *value);
    strcat(cons, "UPDATED");
    *value = cons;
    *val_len = s;
}

void kvolve_declare_update(){
    kvolve_upd_spec("*", "*", 2, 3, 1, test_fun_1);
}

