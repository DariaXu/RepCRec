from data_mgr import DataMgr
from transation_mgr import TransactionMgr
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
            for t in youngest:
                # young die
                # youngest = transMgr.waitLists.get_youngest_transaction()
                transMgr.abort(t, tick)

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
                break
            
        opName, args = exe
        if opName not in operations:
            continue
        op = operations[opName]
        # NOTE: now every operation function should have tick as the last parameter
        logger.debug(f"{tick}: Executing {opName}({args})...")
        if args[0] != '':
            args.append(tick)
            lastResult = op(*args)
        else:
            op()
        tick+=1 

def main():
    parser = argparse.ArgumentParser(description='Replicated Concurrency Control and Recovery.')
    parser.add_argument('testFile', nargs="?", default="tests/test8.txt", help='Test File')
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
    # streamhdlr = logging.StreamHandler()
    # streamhdlr.setFormatter(SpecialFormatter())

    logging.basicConfig(
        # level= logging.INFO,
        level= logging.DEBUG,
        # handlers=[filehdlr1, filehdlr2, streamhdlr],
        handlers=[filehdlr1, filehdlr2]
    )

    DM = DataMgr(NUM_OF_SITES, NUM_OF_VARIABLES)
    TM = TransactionMgr(DM)

    execs = process_input(args.testFile)
    run(execs, DM, TM)
    

if __name__ == "__main__":
    main()