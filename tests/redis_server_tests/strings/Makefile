
all:
	gcc -shared -fPIC -g test_upd_no_ns_change.c -o test_upd_no_ns_change.so -ldl -I../../../redis-2.8.17/src/
	gcc -shared -fPIC -g test_upd_with_ns_change.c -o test_upd_with_ns_change.so -ldl -I../../../redis-2.8.17/src/
	gcc -g run_tests.c -o run_tests -lhiredis 
clean:
	rm -f *so
	rm -f run_tests
