from enum import Enum

NUM_OF_VARIABLES = 20
NUM_OF_SITES = 10

class LockState(str, Enum):
    R_LOCK = 'R_Lock'
    RW_LOCK = 'RW_Lock'

class OperationType(str, Enum):
    READ = "R"
    WRITE = "W"

class ResultType(str, Enum):
    ABORT = "abort"
    WL = "wait_list"
    SUCCESS = "success"
    STOP = "stop"

