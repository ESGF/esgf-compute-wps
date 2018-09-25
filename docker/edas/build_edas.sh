#! /bin/bash

cd edas/

source bin/setup_runtime.sh

sbt package

sbt package
