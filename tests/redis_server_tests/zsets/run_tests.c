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
const char * no_ns_change = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/zsets/test_upd_no_ns_change.so";
const char * w_ns_change = "update/home/ksaur/AY1415/schema_update/tests/redis_server_tests/zsets/test_upd_with_ns_change.so";


void test_sets_nschange(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@6");
  check(101, reply, "OK");

  reply = redisCommand(c,"ZADD %s %s %s %s %s", "order:111", "2", "ffff", "1", "wwww");
  check_int(102, reply, 2);                                     
                                                                
  reply = redisCommand(c,"ZADD %s %s %s %s %s", "order:222", "2", "xxxx", "1", "yyyy");
  check_int(103, reply, 2);                                     
                                                                
  reply = redisCommand(c,"ZADD %s %s %s %s %s", "order:333", "2", "qqqq", "1", "eeee");
  check_int(104, reply, 2);                                     
                                                                
  reply = redisCommand(c,"ZADD %s %s %s %s %s", "order:444", "2", "aaaa", "1", "nnnn");
  check_int(105, reply, 2);

  reply = redisCommand(c,"client setname %s", w_ns_change); 
  check(106, reply, "OK");

  reply = redisCommand(c,"ZRANGE %s %s %s", "foo:order:111", "0", "-1");
  assert(strcmp(reply->element[0]->str, "wwww") == 0  || strcmp(reply->element[0]->str, "ffff") == 0 );
  assert(strcmp(reply->element[1]->str, "wwww") == 0  || strcmp(reply->element[1]->str, "ffff") == 0 );
  freeReplyObject(reply);

  reply = redisCommand(c,"ZADD %s %s %s", "foo:order:222", "3", "zzzz");
  check_int(107, reply, 1);

  /* test that modifying any member of the set (above) applies the update to all members */
  reply = redisCommand(c,"ZSCORE %s %s", "foo:order:222", "xxxx");
  check(108, reply, "2");

  /* test that calling triggers an update */
  reply = redisCommand(c,"ZSCORE %s %s", "foo:order:333", "eeee");
  check(109, reply, "1");

  /* test that calling srem correctly deletes after update*/
  reply = redisCommand(c,"ZREM %s %s", "foo:order:444", "aaaa");
  check_int(110, reply, 1);
  reply = redisCommand(c,"ZSCORE %s %s", "foo:order:444", "aaaa");
  assert(reply->type == REDIS_REPLY_NIL);
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
  reply = redisCommand(c, "client setname %s", "order@5,user@5,region@5");
  check(201, reply, "OK");

  reply = redisCommand(c,"ZADD %s %s %s %s %s", "order:111", "2", "ffff", "1", "wwww");
  check_int(202, reply, 2);                                     
                                                                
  reply = redisCommand(c,"ZADD %s %s %s %s %s", "user:222", "2", "xxxx", "1", "yyyy");
  check_int(203, reply, 2);                                     
                                                                
  reply = redisCommand(c,"ZADD %s %s %s %s %s", "order:333", "2", "qqqq", "1", "eeee");
  check_int(204, reply, 2);                                     
                                                                
  reply = redisCommand(c,"ZADD %s %s %s %s %s", "order:444", "2", "aaaa", "1", "nnnn");
  check_int(205, reply, 2);

  reply = redisCommand(c,"ZADD %s %s %s %s %s", "region:bbb", "2", "aaaa", "1", "tttt");
  check_int(205, reply, 2);

  reply = redisCommand(c,"client setname %s", no_ns_change); 
  check(206, reply, "OK");

  reply = redisCommand(c,"ZRANGE %s %s %s", "order:111", "0", "-1");
  assert(strcmp(reply->element[0]->str, "wwwwUPDATED") == 0  || strcmp(reply->element[0]->str, "ffffUPDATED") == 0 );
  assert(strcmp(reply->element[1]->str, "wwwwUPDATED") == 0  || strcmp(reply->element[1]->str, "ffffUPDATED") == 0 );
  freeReplyObject(reply);

  reply = redisCommand(c,"ZADD %s %s %s", "user:222", "3", "zzzzUPDATED");
  check_int(207, reply, 1);

  /* test that modifying any member of the set (above) applies the update to all members */
  reply = redisCommand(c,"ZSCORE %s %s", "user:222", "xxxxUPDATED");
  check(208, reply, "23");

  /* test that calling sismember triggers an update */
  reply = redisCommand(c,"ZSCORE %s %s", "order:333", "qqqqUPDATED");
  check(209, reply, "2");
  reply = redisCommand(c,"ZSCORE %s %s", "region:bbb", "tttt");
  check(210, reply, "23");

  /* test that calling srem correctly deletes after update*/
  reply = redisCommand(c,"ZREM %s %s", "order:444", "aaaaUPDATED");
  check_int(211, reply, 1);
  reply = redisCommand(c,"ZSCORE %s %s", "order:444", "aaaaUPDATED");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);
  reply = redisCommand(c,"ZSCORE %s %s", "order:444", "aaaa");
  assert(reply->type == REDIS_REPLY_NIL);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);

}
/* could combine with above, but breaking it up*/
void test_int_sets(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@5,user@5");
  check(401, reply, "OK");

  reply = redisCommand(c,"ZADD %s %s %s", "order:111", "3", "2");
  check_int(402, reply, 1);

  reply = redisCommand(c,"client setname %s", w_ns_change);
  check(403, reply, "OK");


  reply = redisCommand(c,"ZRANGE %s %s %s", "foo:order:111", "0", "-1");
  assert(strcmp(reply->element[0]->str, "2") == 0);
  freeReplyObject(reply);

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);

}

void test_sets_valchange2(void){
  redisReply *reply;
  system(server_loc);
  sleep(2);

  redisContext * c = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(c, "client setname %s", "order@5,user@5,region@5");
  check(501, reply, "OK");

  reply = redisCommand(c,"ZADD %s %s %s %s %s", "order:111", "2", "ffff", "1", "wwww");
  check_int(502, reply, 2);                                     
  reply = redisCommand(c,"ZADD %s %s %s %s %s", "region:111", "2", "ffff", "1", "wwww");
  check_int(503, reply, 2);                                     
  reply = redisCommand(c,"ZADD %s %s %s %s %s", "user:111", "2", "ffff", "1", "wwww");
  check_int(504, reply, 2);                                     
  reply = redisCommand(c,"client setname %s", no_ns_change); 
  check(505, reply, "OK");
  reply = redisCommand(c,"ZSCORE %s %s", "order:111", "ffffUPDATED");
  check(506, reply, "2");
  reply = redisCommand(c,"ZSCORE %s %s", "region:111", "ffff");
  check(507, reply, "23");
  reply = redisCommand(c,"ZSCORE %s %s", "user:111", "ffffUPDATED");
  check(508, reply, "23");

  printf("Redis shutdown:\n");
  system("killall redis-server");
  sleep(2);
}

int main(void){

  system("killall redis-server");
  sleep(2);
  test_sets_nschange();
  test_sets_valchange();
  test_int_sets();
  test_sets_valchange2();
  printf("All pass.\n");
  return 0;
}

