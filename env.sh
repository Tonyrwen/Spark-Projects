#!/bin/bash

echo "Please enter the internal IP of your manager node:"
read IP
export SPARK_MASTER=${IP}
