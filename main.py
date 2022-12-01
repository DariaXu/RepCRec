from Data_Mgr import DataMgr
from Transation_Mgr import TransactionMgr
from const import NUM_OF_SITES, NUM_OF_VARIABLES, OperationType, ResultType
import argparse
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def process_input(file):
    executions = []
    with open(file) as input:
        for line in input:
            if line.startswith("//") or not line.find('(') or not line.find(')'):
                continue

            openB = line.find('(')
            closeB = line.find(')')
            operation = line[:openB]
            vars = line[openB+1:closeB].split(',')
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
    }

    tick = 0
    lastResult = None
    for i, exec in enumerate(executions):
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
                transMgr.waitLists.remove_last_from_waitList()
            else:
                logger.debug(f"{tick}: Successfully executed {str(waitObj)} from wait list")
                lastResult = result
                transMgr.waitLists.remove_from_waitList(waitObj)
                tick += 1
                break
            
        opName, args = exec
        if opName not in operations:
            continue
        op = operations[opName]
        # NOTE: now every operation function should have tick as the last parameter
        logger.debug(f"{tick}: Executing {opName}({args})...")
        args.append(tick)
        lastResult = op(*args)
        tick+=1 

def main():
    parser = argparse.ArgumentParser(description='Replicated Concurrency Control and Recovery.')
    parser.add_argument('testFile', nargs="?", default="tests/test2.txt", help='Test File')
    args = parser.parse_args()

    # curDatetime = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
    testName = Path(args.testFile).stem
    logging.basicConfig(
        level= logging.INFO,
        # level= logging.DEBUG,
        format='%(levelname)s: [%(module)s] %(message)s',
        handlers=[
            logging.FileHandler(f"logs/{testName}.log", mode='w'),
            logging.StreamHandler()
        ]
    )

    DM = DataMgr(NUM_OF_SITES, NUM_OF_VARIABLES)
    TM = TransactionMgr(DM)

    execs = process_input(args.testFile)
    run(execs, DM, TM)
    

if __name__ == "__main__":
    main()