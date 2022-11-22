#!/bin/bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"
cd ..

make build
echo "Built relationalize package."
cd test

rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip3 install "../dist/$(ls -AU ../dist | head -1)"

EXIT_CODE_SUM=0
for test_file in $(ls *.test.py); do
    echo "RUNNING TEST $test_file"
    python3 $test_file || EXIT_CODE_SUM=$(($EXIT_CODE_SUM + $?))
done

if [ $EXIT_CODE_SUM -ne 0 ]; then
    echo "THERE WERE ISSUES WITH TESTS."
fi

deactivate
rm -rf .venv

exit $EXIT_CODE_SUM
