#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "kvolve_upd.h"



void test_hashkeychange_only(char ** key, void ** value, size_t * val_len){
    struct hash_subkeyval *hsk = (struct hash_subkeyval*) value;
    size_t s = strlen("UPDATED")+strlen(hsk->hashkey)+1;
    char * cons = calloc(s,sizeof(char));
    strcat(cons, hsk->hashkey);
    strcat(cons, "UPDATED");
    hsk->hashkey = cons;
}

void test_hashvalchange_only(char ** key, void ** value, size_t * val_len){
    struct hash_subkeyval *hsk = (struct hash_subkeyval*) value;
    size_t s2 = strlen("UPDATED")+strlen(hsk->hashval)+1;
    char * cons = calloc(s2,sizeof(char));
    strcat(cons, hsk->hashval);
    strcat(cons, "UPDATED");
    hsk->hashval = cons;
}

void test_bothchange(char ** key, void ** value, size_t * val_len){
    struct hash_subkeyval *hsk = (struct hash_subkeyval*) value;
    size_t s = strlen("UPDATED")+strlen(hsk->hashkey)+1;
    char * cons = calloc(s,sizeof(char));
    strcat(cons, hsk->hashkey);
    strcat(cons, "UPDATED");
    hsk->hashkey = cons;
    size_t s2 = strlen("UPDATED")+strlen(hsk->hashval)+1;
    char * cons2 = calloc(s2,sizeof(char));
    strcat(cons2, hsk->hashval);
    strcat(cons2, "UPDATED");
    hsk->hashval = cons2;
}


void kvolve_declare_update(){
    kvolve_upd_spec("order", "order", 5, 6, 1, test_hashkeychange_only);
    kvolve_upd_spec("region", "region", 5, 6, 1, test_hashvalchange_only);
    kvolve_upd_spec("user", "user", 5, 6, 1, test_bothchange);
}

