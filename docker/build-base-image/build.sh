#!/bin/bash

make deb 
rm -f /src/dist/*.deb
mv /src/*.deb /src/dist

