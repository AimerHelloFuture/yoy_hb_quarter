#!/usr/bin/env bash
export PATH=/usr/local/python-2.7/bin:$PATH

nohup python analyseservice.py job "$@" &