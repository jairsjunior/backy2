#!/bin/bash

backy2 initdb
dd if=/dev/urandom of=/testfile bs=4M count=10
backy2 backup file://testfile testbackup1