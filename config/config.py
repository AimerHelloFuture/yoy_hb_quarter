#! /usr/bin/env python
# -*- coding: utf-8 -*-
from os import makedirs
from os import path
from os.path import dirname

import env

import config_default

configs = config_default.configs

try:
    ONLINE = env.ENV and env.ENV == 'online'
    ONLINE_DEV = env.ENV and env.ENV == 'online_dev'

    if ONLINE:
        import config_online
        configs = config_online.configs
    elif ONLINE_DEV:
        import config_online_dev
        configs = config_online_dev.configs

    # file folder path
    for file_folder in configs['file'].values():
        if not path.isdir(file_folder):
            makedirs(file_folder)

    # pid_file
    for file in configs['pid'].values():
        base_dir = dirname(file)
        if not path.isdir(base_dir):
            makedirs(base_dir)

except ImportError:
    pass
