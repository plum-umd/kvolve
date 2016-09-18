#!/bin/bash
cd misc
echo "MISC"
make clean; rm core; make ;./run_tests
cd ../sets
echo "SETS"
make clean; rm core; make ;./run_tests
cd ../zsets
echo "ZSETS"
make clean; rm core; make ;./run_tests
cd ../strings
echo "STRINGS"
make clean; rm core; make ;./run_tests
cd ../lists
echo "LISTS"
make clean; rm core; make ;./run_tests
cd ../hashes
echo "HASHES"
make clean; rm core; make ;./run_tests
cd ..
echo "DONE"
