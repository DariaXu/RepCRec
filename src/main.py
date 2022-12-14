"""
Main scripts to run the implementation of the distributed database system.

This is the final project of CSCI-GA 2423-001 (Advanced Database Systems, Fall 2022) 
at New York University. This project aims to implement a distributed database system, 
which has following features:  

(1) multiversion concurrency control;  
(2) deadlock detection (cycle detection with youngest victim selection);  
(3) replication (available copies algorithm using strict two phase locking);  
(4) failure recovery (under available copies algorithm).  


Typical usage example (run a single input test):

    python3 main.py   \
        --testFile <PathToTestFile>  \
        --stdout  # remove this arg to not printing out the results

@Author: Tanran Zheng (tz408@nyu.edu) and Daria Xu (xx2085@nyu.edu).
@Date: Dec/03/2022
@Instructor: Prof. Dennis Shasha

"""

from data_mgr import DataMgr
from transaction_mgr import TransactionMgr
from const import NUM_OF_SITES, NUM_OF_VARIABLES, OperationType, ResultType
import argparse
import logging
from pathlib import Path
import utils as utils

logger = logging.getLogger(__name__)

class SpecialFormatter(logging.Formatter):
    FORMATS = { logging.INFO: "%(message)s",
               'DEFAULT': '%(levelname)s: [%(module)s] %(message)s'}

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno, self.FORMATS['DEFAULT'])
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def process_input(file):
    executions = []
    with open(file) as input:
        for line in input:
            if line.startswith("//") or not line.find('(') or not line.find(')'):
                continue

            openB = line.find('(')
            closeB = line.find(')')
            operation = line[:openB]
            vars = [v.strip() for v in line[openB+1:closeB].split(',')]
            executions.append((operation, vars))

    return executions

def run(executions, dataMgr, transMgr):
    operations = {
        # add new operation function to this dict
        "begin": transMgr.start_transaction,
        "beginRO": transMgr.start_RO_transaction,
        "fail": dataMgr.fail,
        "recover": dataMgr.recover,
        OperationType.READ: transMgr.read,
        OperationType.WRITE: transMgr.write,
        "end": transMgr.end,
        "dump": transMgr.dump,
    }

    tick = 0
    lastResult = None
    for exe in executions:
        if lastResult == ResultType.WL:
            logger.debug(f"{tick}: Detecting deadlock...")
            youngest = transMgr.waitLists.deadlock_detection()
            while youngest:
                for t in youngest:
                    # young die
                    # youngest = transMgr.waitLists.get_youngest_transaction()
                    transMgr.abort(t, tick)
                youngest = transMgr.waitLists.deadlock_detection()

        # multiple executing wait list
        executingWL = True
        while executingWL:
            executingWL = False
            for waitObj in list(transMgr.waitLists.get_waitList()):
                logger.debug(f"{tick}: Trying to execute {str(waitObj)} from wait list...")
                opName, args = waitObj.operation
                if opName not in operations:
                    continue
                op = operations[opName]

                nerArgs = args + [tick]
                result = op(*nerArgs)
                if result == ResultType.WL:
                    # keep the original wait object
                    logger.debug(f"{tick}: Failed to execute {str(waitObj)} from wait list")
                    # transMgr.waitLists.remove_last_from_waitList()
                else:
                    logger.debug(f"{tick}: Executed {str(waitObj)} from wait list")
                    lastResult = result
                    transMgr.waitLists.remove_from_waitList(waitObj)
                    tick += 1
                    executingWL = True

        # stop after successful execute one operation from wait list
        # for waitObj in list(transMgr.waitLists.get_waitList()):
        #     logger.debug(f"{tick}: Trying to execute {str(waitObj)} from wait list...")
        #     opName, args = waitObj.operation
        #     if opName not in operations:
        #         continue
        #     op = operations[opName]

        #     nerArgs = args + [tick]
        #     result = op(*nerArgs)
        #     if result == ResultType.WL:
        #         # keep the original wait object
        #         logger.debug(f"{tick}: Failed to execute {str(waitObj)} from wait list")
        #         # transMgr.waitLists.remove_last_from_waitList()
        #     else:
        #         logger.debug(f"{tick}: Executed {str(waitObj)} from wait list")
        #         lastResult = result
        #         transMgr.waitLists.remove_from_waitList(waitObj)
        #         tick += 1
        #         break

        opName, args = exe
        if opName not in operations:
            continue
        op = operations[opName]
        # NOTE: now every operation function should have tick as the last parameter
        logger.debug(f"{tick}: Executing {opName}({args})...")
        if opName == 'dump' and args[0] == '':
            op()
        elif opName != 'dump':
            args.append(tick)
            lastResult = op(*args)
        else:
            op(*args)
        tick+=1 

def main():
    parser = argparse.ArgumentParser(description='Replicated Concurrency Control and Recovery.')
    parser.add_argument('--testFile', type=str, default="tests/test1.txt", help='Path to test file.')
    parser.add_argument('--stdout', nargs='?', type=utils.str_to_bool, const=True, default=False)

    args = parser.parse_args()
    
    utils.mkdir("./logs")
    utils.mkdir("./output")
    # curDatetime = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
    testName = Path(args.testFile).stem

    # logger setting
    filehdlr1 = logging.FileHandler(f"logs/{testName}.log", mode='w')
    filehdlr1.setLevel(logging.DEBUG)
    filehdlr1.setFormatter(SpecialFormatter())

    filehdlr2 = logging.FileHandler(f"output/{testName}.out", mode='w')
    filehdlr2.setLevel(logging.INFO)
    filehdlr2.setFormatter(SpecialFormatter())

    # std out
    if args.stdout:
        streamhdlr = logging.StreamHandler()
        streamhdlr.setFormatter(SpecialFormatter())
        handler = [filehdlr1, filehdlr2, streamhdlr]
    else:
        handler = [filehdlr1, filehdlr2]

    logging.basicConfig(
        # level= logging.INFO,
        level= logging.DEBUG,
        handlers=handler
    )

    DM = DataMgr(NUM_OF_SITES, NUM_OF_VARIABLES)
    TM = TransactionMgr(DM)

    execs = process_input(args.testFile)
    run(execs, DM, TM)
    

if __name__ == "__main__":
    main()