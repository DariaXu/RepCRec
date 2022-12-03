"""
Script that contains the class that contains the utility functions.

@Author: Tanran Zheng (tz408@nyu.edu).
@Date: Dec/03/2022
@Instructor: Prof. Dennis Shasha

"""

import errno
import os

def mkdir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in {'false', 'f', '0', 'no', 'n'}:
        return False
    elif value.lower() in {'true', 't', '1', 'yes', 'y'}:
        return True
    raise ValueError(f'{value} is not a valid boolean value')