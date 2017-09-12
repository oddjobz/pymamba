#!/bin/bash
if ! [ -a .build ] ; then
        echo "Creating Virtual Environment"
        virtualenv .build -p python3
fi
source .build/bin/activate
pip3 install -r requirements.txt
export PYTHONPATH=.
