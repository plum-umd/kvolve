#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <hiredis/hiredis.h>

#define DEBUG


void check(int test_num, redisReply *reply, char * expected){
#ifdef DEBUG
  printf("(%d) Expected: %s, Got: %s\n", test_num, expected, reply->str);
#endif
  assert(strncmp(reply->str, expected, strlen(expected))==0);
  freeReplyObject(reply);
}


void do_upd(char * name, char * loc){
  redisReply *reply;

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", name);
  check(1, reply, "OK");

  reply = redisCommand(c,"client setname %s", loc);
  check(2, reply, "OK");

}

int main(int argc, char *argv[]){

  if(argc!=3){
    printf("usage: ./run_upd connection_args upd_loc\n");
    return -1;
  }
  do_upd(argv[1],argv[2]);
  return 0;
}
