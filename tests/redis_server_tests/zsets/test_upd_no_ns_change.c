#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "kvolve_upd.h"

void test_keychange_only(char ** key, void ** value, size_t * val_len){
    struct zset_scoreval * sv = (struct zset_scoreval*)value;
    size_t s = strlen("UPDATED")+strlen(sv->setelem)+1;
    char * cons = calloc(s,sizeof(char));
    strcat(cons, sv->setelem);
    strcat(cons, "UPDATED");
    sv->setelem = cons;

}
void test_scorechange_only(char ** key, void ** value, size_t * val_len){
    double newscore = 23;
    struct zset_scoreval * sv = (struct zset_scoreval*)value;
    sv->score = &newscore;

}

void test_bothchange(char ** key, void ** value, size_t * val_len){
    double newscore = 23;
    struct zset_scoreval * sv = (struct zset_scoreval*)value;
    size_t s = strlen("UPDATED")+strlen(sv->setelem)+1;
    char * cons = calloc(s,sizeof(char));
    strcat(cons, sv->setelem);
    strcat(cons, "UPDATED");
    sv->setelem = cons;
    sv->score = &newscore;
}


void kvolve_declare_update(){
    kvolve_upd_spec("order", "order", 5, 6, 1, test_keychange_only);
    kvolve_upd_spec("region", "region", 5, 6, 1, test_scorechange_only);
    kvolve_upd_spec("user", "user", 5, 6, 1, test_bothchange);
}

