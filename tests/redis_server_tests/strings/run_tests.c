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
const char * no_ns_change = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/strings/test_upd_no_ns_change.so";
const char * w_ns_change = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/strings/test_upd_with_ns_change.so";


/* Test updating with two namespaces.  Perform the updates on the two
 * keys.  Then load the 2nd udpate (namespace change), and perform the 
 * update.  Assert that the key at the old ns was deleted. */
void test_update_separate(void){

  redisReply *reply;

  system(server_loc);
  sleep(2);
  printf("Inside test_update_separate\n");

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@5,user@5");
  check(1, reply, "OK");

  reply = redisCommand(c,"SET %s %s", "order:111", "ffff");
  check(2, reply, "OK");
  reply = redisCommand(c,"SET %s %s", "order:222", "ffff");
  check(2, reply, "OK");
  reply = redisCommand(c,"SET %s %s", "user:bbbb", "9999");
  check(3, reply, "OK");

  printf("about to load update\n");
  reply = redisCommand(c,"client setname %s", no_ns_change); 
  check(4, reply, "OK");

  printf("done loading update\n");
  reply = redisCommand(c,"GET %s", "order:111");
  check(5, reply, "ffffUPDATED");
  reply = redisCommand(c,"GET %s", "user:bbbb");
  check(6, reply, "9999");

  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(7, reply, "OK");
  reply = redisCommand(c,"GET %s", "foo:order:111");
  check(8, reply, "ffffUPDATED");

  reply = redisCommand(c,"GET %s", "order:111");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);

  /* test that old version is clobbered by set in ns change*/
  reply = redisCommand(c,"SET %s %s", "foo:order:222", "eeee");
  check(9, reply, "OK");

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 3);
  freeReplyObject(reply);
   
  system("killall redis-server");
  sleep(2);
 
}


/* The same test as before, except test running the updates one after the
 * other.*/
void test_update_consecu(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@5,user@5");
  check(101, reply, "OK");

  reply = redisCommand(c,"SET %s %s", "order:111", "ffff");
  check(102, reply, "OK");

  reply = redisCommand(c,"client setname %s", no_ns_change); 
  check(103, reply, "OK");
  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(104, reply, "OK");

  reply = redisCommand(c,"GET %s", "foo:order:111");
  check(105, reply, "ffffUPDATED");

  reply = redisCommand(c,"GET %s", "order:111");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 1);
  freeReplyObject(reply);

  system("killall redis-server");
  sleep(2);
}

void test_nx(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(201, reply, "OK");

  reply = redisCommand(c,"SET %s %s", "order:111", "ffff");
  check(202, reply, "OK");

  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(203, reply, "OK");

  reply = redisCommand(c,"SET %s %s %s", "foo:order:111", "gggg", "nx");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);

  // Make sure that the name got changed
  reply = redisCommand(c,"GET %s", "order:111");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);

  // Make sure that the VALUE did NOT get changed (should be ffff not gggg).
  reply = redisCommand(c,"GET %s", "foo:order:111");
  check(204, reply, "ffff");

  // Make sure it still works without ns change
  reply = redisCommand(c,"SET %s %s %s", "foo:order:111", "pppp", "nx");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);

  reply = redisCommand(c,"GET %s", "foo:order:111");
  check(205, reply, "ffff");

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 1);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}
/* Redis allows "setnx" as an actual (to-be-deprecated) command or "set ... nx" as flags
   http://redis.io/commands/set
   http://redis.io/commands/setnx */
void test_setnx(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(301, reply, "OK");

  reply = redisCommand(c,"SET %s %s", "order:111", "ffff");
  check(302, reply, "OK");

  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(303, reply, "OK");

  reply = redisCommand(c,"SETNX %s %s", "foo:order:111", "gggg");
  assert(reply->type == REDIS_REPLY_INTEGER);
  assert(reply->integer == 0);
  freeReplyObject(reply);

  // Make sure that the name got changed
  reply = redisCommand(c,"GET %s", "order:111");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);

  // Make sure that the VALUE did NOT get changed (should be ffff not gggg).
  reply = redisCommand(c,"GET %s", "foo:order:111");
  check(304, reply, "ffff");

  // Make sure it still works without ns change
  reply = redisCommand(c,"SETNX %s %s", "foo:order:111", "pppp");
  assert(reply->type == REDIS_REPLY_INTEGER);
  assert(reply->integer == 0);
  freeReplyObject(reply);

  reply = redisCommand(c,"GET %s", "foo:order:111");
  check(305, reply, "ffff");

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 1);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}
void test_xx(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(206, reply, "OK");

  reply = redisCommand(c,"SET %s %s", "order:111", "ffff");
  check(207, reply, "OK");

  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(208, reply, "OK");

  reply = redisCommand(c,"SET %s %s %s", "foo:order:111", "gggg", "xx");
  check(209, reply, "OK");

  // Make sure that the name got changed
  reply = redisCommand(c,"GET %s", "order:111");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);

  // Make sure that the value DID get changed 
  reply = redisCommand(c,"GET %s", "foo:order:111");
  check(210, reply, "gggg");

  // Make sure it still works without ns change
  reply = redisCommand(c,"SET %s %s %s", "foo:order:111", "pppp", "xx");
  check(211, reply, "OK");

  reply = redisCommand(c,"GET %s", "foo:order:111");
  check(212, reply, "pppp");

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 1);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

void test_mset_mget(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(301, reply, "OK");

  reply = redisCommand(c,"MSET %s %s %s %s", "order:111", "ffff", "order:222", "wwww");
  check(302, reply, "OK");

  reply = redisCommand(c,"MSET %s %s %s %s", "order:333", "ffff", "order:444", "wwww");
  check(303, reply, "OK");

  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(304, reply, "OK");

  reply = redisCommand(c,"GET %s", "foo:order:111");
  check(305, reply, "ffff");
  reply = redisCommand(c,"GET %s", "foo:order:222");
  check(306, reply, "wwww");

  // Make sure that the old got deleted
  reply = redisCommand(c,"GET %s", "order:111");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);
  reply = redisCommand(c,"GET %s", "order:222");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);
  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 4);
  freeReplyObject(reply);

  reply = redisCommand(c,"MGET %s %s", "foo:order:333", "foo:order:444");
  assert(reply->elements == 2);
  assert(strcmp(reply->element[0]->str, "ffff") == 0);
  assert(strcmp(reply->element[1]->str, "wwww") == 0);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

void test_getrange(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(401, reply, "OK");

  reply = redisCommand(c,"SET %s %s", "order:111", "this is a string");
  check(402, reply, "OK");

  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(403, reply, "OK");

  reply = redisCommand(c,"GETRANGE %s %s %s", "foo:order:111", "0", "3");
  check(404, reply, "this");

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

void test_del(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(501, reply, "OK");

  reply = redisCommand(c,"MSET %s %s %s %s %s %s %s %s", "order:111", "this is a string", "order:222", "wwww", "order:333", "ppp", "order:444", "ooo");
  check(502, reply, "OK");

  reply = redisCommand(c,"DEL %s", "order:444");
  assert(reply->integer == 1);
  freeReplyObject(reply);

  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(503, reply, "OK");

  reply = redisCommand(c,"DEL %s %s", "foo:order:111", "foo:order:333");
  assert(reply->integer == 2);
  freeReplyObject(reply);

  reply = redisCommand(c,"GET %s", "order:111");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);
  reply = redisCommand(c,"GET %s", "order:333");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);

  reply = redisCommand(c,"GET %s", "foo:order:222");
  check(505, reply, "wwww");

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

void test_incr(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(601, reply, "OK");

  reply = redisCommand(c, "INCR %s ", "order:111");
  check_int(602, reply, 1);

  reply = redisCommand(c, "INCR %s ", "order:111");
  check_int(603, reply, 2);

  reply = redisCommand(c, "INCRBY %s %s", "order:222", "10");
  check_int(604, reply, 10);


  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(605, reply, "OK");

  reply = redisCommand(c, "INCR %s ", "foo:order:111");
  check_int(606, reply, 3);

  reply = redisCommand(c, "INCRBY %s %s", "foo:order:222", "10");
  check_int(607, reply, 20);

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 2);
  freeReplyObject(reply);

  reply = redisCommand(c,"DEL %s", "foo:order:222");
  assert(reply->integer == 1);
  freeReplyObject(reply);

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 1);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

void test_getset(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(701, reply, "OK");

  /* next 3 calls verbatim from getset manual entry. Make sure the weird incr works 
   * w/o versioning */
  reply = redisCommand(c, "INCR %s ", "order:111");
  check_int(702, reply, 1);

  reply = redisCommand(c, "GETSET %s %s", "order:111", "0");
  check(703, reply, "1");

  reply = redisCommand(c, "GET %s", "order:111");
  check(704, reply, "0");

  reply = redisCommand(c, "INCR %s ", "order:111");
  check_int(705, reply, 1);

  reply = redisCommand(c,"client setname %s", w_ns_change);
  check(706, reply, "OK");

  reply = redisCommand(c, "GETSET %s %s", "foo:order:111", "33");
  check(707, reply, "1");

  reply = redisCommand(c, "GETSET %s %s", "foo:order:111", "0");
  check(708, reply, "33");

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 1);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

void test_int_encoding(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(801, reply, "OK");

  reply = redisCommand(c, "SET %s  %s", "order:111", "4");
  check(802, reply, "OK");

  reply = redisCommand(c, "GET %s", "order:111");
  check(803, reply, "4");

  reply = redisCommand(c,"client setname %s", w_ns_change);
  check(804, reply, "OK");

  reply = redisCommand(c, "GET %s", "foo:order:111");
  check(805, reply, "4");

  reply = redisCommand(c,"keys %s", "*");
  assert(reply->elements == 1);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}


int main(void){

  system("killall redis-server");
  sleep(2);
  test_update_separate();
  test_update_consecu();
  test_nx();
  test_xx();
  test_setnx();
  test_mset_mget();
  test_getrange();
  test_del();
  test_incr();
  test_getset();
  test_int_encoding();
  printf("All pass.\n");
  return 0;
}
