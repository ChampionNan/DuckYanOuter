#!/bin/bash

trap 'echo "Interrupted"; kill 0; exit 130' INT

bash auto_run.sh rstk outerjoin_test/association1
bash auto_run.sh rstk outerjoin_test/association2
bash auto_run.sh rstk outerjoin_test/simplification
bash auto_run.sh rstk outerjoin_test/desimplification
bash auto_run.sh rstk outerjoin_test/count
bash auto_run.sh rstk outerjoin_test/aggregation
