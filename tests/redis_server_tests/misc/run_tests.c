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
const char * user_call = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/misc/user_call.so";
const char * global_ns = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/misc/global_ns.so";
const char * no_ns_change = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/strings/test_upd_no_ns_change.so";
const char * w_ns_change = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/strings/test_upd_with_ns_change.so";



void test_client_call(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@5");
  check(1, reply, "OK");

  reply = redisCommand(c, "SET %s  %s", "order:111", "oooo");
  check(2, reply, "OK");

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 1);
  freeReplyObject(reply);

  reply = redisCommand(c,"client setname %s", user_call);
  check(3, reply, "OK");

  /* This get should trigger the creation of the other key */
  reply = redisCommand(c, "GET %s", "order:111");
  check(4, reply, "oooo");

  reply = redisCommand(c, "GET %s", "order:222");
  check(5, reply, "wwwww");

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 2);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

void test_default_ns(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);

  reply = redisCommand(c, "SET %s  %s", "test", "oooo");
  check(101, reply, "OK");

  reply = redisCommand(c, "SADD %s  %s", "testset", "oooo");
  check_int(102, reply, 1);

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 2);
  freeReplyObject(reply);

  reply = redisCommand(c, "GET %s", "test");
  check(103, reply, "oooo");

  reply = redisCommand(c, "SMEMBERS  %s", "testset");
  assert(strcmp(reply->element[0]->str, "oooo") == 0);
  freeReplyObject(reply);

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 2);
  freeReplyObject(reply);

  reply = redisCommand(c, "SET %s  %s", "test:nonethanks", "oooo");
  check(104, reply, "OK");

  reply = redisCommand(c, "GET %s", "test:nonethanks");
  check(105, reply, "oooo");

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

void test_keys(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@5");
  check(201, reply, "OK");

  reply = redisCommand(c, "MSET  %s %s  %s %s  %s %s  %s %s", "order:1",  "ffff", "order:2", "f", "user:b", "9", "order:11",  "x"); 
  check(201, reply, "OK");

  reply = redisCommand(c,"client setname %s", no_ns_change); 
  check(203, reply, "OK");

  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(204, reply, "OK");

  reply = redisCommand(c,"keys %s", "foo:order:1*");
  assert(reply->elements == 2);
  freeReplyObject(reply);

  reply = redisCommand(c,"GET %s", "foo:order:1");
  check(205, reply, "ffffUPDATED");

  reply = redisCommand(c,"GET %s", "order:1");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);


  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

void test_globalns(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "*@2");
  check(301, reply, "OK");

  reply = redisCommand(c, "MSET  %s %s  %s %s  %s %s  %s %s", "foo1",  "ffff", "foo2", "f", "userb", "piano", "foo11",  "x"); 
  check(302, reply, "OK");

  reply = redisCommand(c,"client setname %s", global_ns); 
  check(303, reply, "OK");

  reply = redisCommand(c,"GET %s", "foo1");
  check(304, reply, "ffffUPDATED");

  reply = redisCommand(c,"GET %s", "userb");
  check(305, reply, "pianoUPDATED");

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

int main(void){

  system("killall redis-server");
  sleep(2);
  test_client_call();
  test_default_ns();
  test_keys();
  test_globalns();
  printf("All pass.\n");
  return 0;
}
