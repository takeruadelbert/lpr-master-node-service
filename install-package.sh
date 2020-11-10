#!/bin/sh

virtualenv venv -q -p /usr/bin/python3.8
source venv/bin/activate
venv/bin/pip install -r requirement/requirements.txt
