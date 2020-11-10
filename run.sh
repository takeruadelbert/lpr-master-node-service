#!/bin/sh
export $(cat environment.env | xargs)

python3 main.py
