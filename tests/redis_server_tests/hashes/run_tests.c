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

void check_int(int test_num, redisReply *reply, int expected){
#ifdef DEBUG
  printf("(%d) Expected: %d, Got: %lld\n", test_num, expected, reply->integer);
#endif
  assert(reply->integer == expected);
  freeReplyObject(reply);
}


const char * server_loc = "../../../redis-2.8.17/src/redis-server ../../../redis-2.8.17/redis.conf &";
const char * no_ns_change = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/hashes/test_upd_no_ns_change.so";
const char * w_ns_change = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/hashes/test_upd_with_ns_change.so";


void test_hashes_nschange(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(101, reply, "OK");

  reply = redisCommand(c,"HSET %s %s %s", "order:111", "f1", "v1");
  check_int(102, reply, 1);

  reply = redisCommand(c,"HMSET %s %s %s %s %s", "order:222", "f3", "v3", "f4", "v4");
  check(103, reply, "OK");

  reply = redisCommand(c,"HMSET %s %s %s %s %s", "order:333", "f5", "v5", "f6", "v6");
  check(104, reply, "OK");

  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(106, reply, "OK");

  reply = redisCommand(c,"HGET %s %s", "foo:order:111", "f1");
  check(107, reply, "v1");

  reply = redisCommand(c,"HMGET %s %s %s", "foo:order:222", "f3", "f4");
  assert(strcmp(reply->element[0]->str, "v3") == 0  || strcmp(reply->element[0]->str, "v4") == 0 );
  assert(strcmp(reply->element[1]->str, "v3") == 0  || strcmp(reply->element[1]->str, "v4") == 0 );

  reply = redisCommand(c,"HDEL %s %s", "foo:order:333", "f5");
  check_int(107, reply, 1);

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 3);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);

}

void test_hashes_valchange(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@5,user@5,region@5");
  check(201, reply, "OK");

  reply = redisCommand(c,"HMSET %s %s %s %s %s", "order:111", "f1", "v1", "f2", "v2");
  check(202, reply, "OK");

  reply = redisCommand(c,"HMSET %s %s %s %s %s", "region:111", "f1", "v1", "f2", "v2");
  check(203, reply, "OK");

  reply = redisCommand(c,"HMSET %s %s %s %s %s", "user:111", "f1", "v1", "f2", "v2");
  check(204, reply, "OK");

  reply = redisCommand(c,"client setname %s", no_ns_change); 
  check(205, reply, "OK");

  reply = redisCommand(c,"HGETALL %s", "order:111");
  assert(strcmp(reply->element[0]->str, "f1UPDATED") == 0  || strcmp(reply->element[0]->str, "f2UPDATED") == 0 );
  assert(strcmp(reply->element[2]->str, "f1UPDATED") == 0  || strcmp(reply->element[2]->str, "f2UPDATED") == 0 );
  freeReplyObject(reply);
  printf("+=======+\n");

  reply = redisCommand(c,"HGETALL %s", "region:111");
  assert(strcmp(reply->element[1]->str, "v1UPDATED") == 0  || strcmp(reply->element[1]->str, "v2UPDATED") == 0 );
  assert(strcmp(reply->element[3]->str, "v1UPDATED") == 0  || strcmp(reply->element[3]->str, "v2UPDATED") == 0 );
  freeReplyObject(reply);
  printf("+=======+\n");

  reply = redisCommand(c,"HGETALL %s", "user:111");
  assert(strcmp(reply->element[0]->str, "f1UPDATED") == 0  || strcmp(reply->element[0]->str, "f2UPDATED") == 0 );
  assert(strcmp(reply->element[2]->str, "f1UPDATED") == 0  || strcmp(reply->element[2]->str, "f2UPDATED") == 0 );
  assert(strcmp(reply->element[1]->str, "v1UPDATED") == 0  || strcmp(reply->element[1]->str, "v2UPDATED") == 0 );
  assert(strcmp(reply->element[3]->str, "v1UPDATED") == 0  || strcmp(reply->element[3]->str, "v2UPDATED") == 0 );
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);

}
///* could combine with above, but breaking it up*/

int main(void){

  system("killall redis-server");
  sleep(2);
  test_hashes_nschange();
  test_hashes_valchange();
  printf("All pass.\n");
  return 0;
}
