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
const char * no_ns_change = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/lists/test_upd_no_ns_change.so";
const char * w_ns_change = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/lists/test_upd_with_ns_change.so";


void test_sets_nschange(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(101, reply, "OK");

  reply = redisCommand(c,"LPUSH %s %s %s", "order:111", "ffff", "wwww");
  check_int(102, reply, 2);

  reply = redisCommand(c,"LRANGE %s %s %s", "order:111", "0", "-1");
  assert(strcmp(reply->element[0]->str, "wwww") == 0);
  assert(strcmp(reply->element[1]->str, "ffff") == 0 );
  freeReplyObject(reply);


  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(103, reply, "OK");

  reply = redisCommand(c,"LRANGE %s %s %s", "foo:order:111", "0", "-1");
  assert(strcmp(reply->element[0]->str, "wwww") == 0);
  assert(strcmp(reply->element[1]->str, "ffff") == 0 );
  freeReplyObject(reply);

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 1);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

void test_sets_valchange(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@5,user@5");
  check(201, reply, "OK");

  reply = redisCommand(c,"LPUSH %s %s %s", "order:111", "ffff", "wwww");
  check_int(202, reply, 2);

  reply = redisCommand(c,"LRANGE %s %s %s", "order:111", "0", "-1");
  assert(strcmp(reply->element[0]->str, "wwww") == 0);
  assert(strcmp(reply->element[1]->str, "ffff") == 0 );
  freeReplyObject(reply);


  reply = redisCommand(c,"client setname %s", no_ns_change); 
  check(203, reply, "OK");

  reply = redisCommand(c,"LRANGE %s %s %s", "order:111", "0", "-1");
  assert(strcmp(reply->element[0]->str, "wwwwUPDATED") == 0 );
  assert(strcmp(reply->element[1]->str, "ffffUPDATED") == 0 );
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);

}

void test_llen(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@5,user@5");
  check(301, reply, "OK");

  reply = redisCommand(c,"RPUSH %s %s %s", "order:111", "ffff", "wwww");
  check_int(302, reply, 2);

  reply = redisCommand(c,"LRANGE %s %s %s", "order:111", "0", "-1");
  assert(strcmp(reply->element[0]->str, "ffff") == 0);
  assert(strcmp(reply->element[1]->str, "wwww") == 0 );
  freeReplyObject(reply);


  reply = redisCommand(c,"client setname %s", no_ns_change); 
  check(303, reply, "OK");
  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(304, reply, "OK");

  reply = redisCommand(c,"LLEN %s", "foo:order:111");
  check_int(305, reply, 2);

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 1);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);

}

void test_lset_pop(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@5,user@5");
  check(401, reply, "OK");

  reply = redisCommand(c,"RPUSH %s %s %s", "order:111", "ffff", "wwww");
  check_int(402, reply, 2);

  reply = redisCommand(c,"LPUSH %s %s %s", "order:222", "ffff", "wwww");
  check_int(403, reply, 2);

  reply = redisCommand(c,"RPUSH %s %s %s", "order:333", "ffff", "wwww");
  check_int(404, reply, 2);


  reply = redisCommand(c,"client setname %s", no_ns_change); 
  check(405, reply, "OK");
  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(406, reply, "OK");

  reply = redisCommand(c,"LSET %s %s %s", "foo:order:111", "0", "ppppUPDATED");
  check(407, reply, "OK");

  reply = redisCommand(c,"LRANGE %s %s %s", "foo:order:111", "0", "-1");
  assert(strcmp(reply->element[0]->str, "ppppUPDATED") == 0);
  assert(strcmp(reply->element[1]->str, "wwwwUPDATED") == 0 );
  freeReplyObject(reply);

  reply = redisCommand(c,"LLEN %s", "foo:order:111");
  check_int(408, reply, 2);

  reply = redisCommand(c,"LPOP %s", "foo:order:222");
  assert(strcmp(reply->str, "wwwwUPDATED") == 0 );
  freeReplyObject(reply);

  reply = redisCommand(c,"RPOP %s", "foo:order:333");
  assert(strcmp(reply->str, "wwwwUPDATED") == 0 );
  freeReplyObject(reply);

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 3);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);

}

int main(void){

  system("killall redis-server");
  sleep(2);
  test_sets_nschange();
  test_sets_valchange();
  test_llen();
  test_lset_pop();
  printf("All pass.\n");
  return 0;
}
