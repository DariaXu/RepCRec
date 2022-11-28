from Data_Mgr import DataMgr
from Transation_Mgr import TransactionMgr
from const import NUM_OF_SITES, NUM_OF_VARIABLES
import argparse
import logging
import datetime

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
    }

    tick = 0
    for i, exec in enumerate(executions):
        opName, args = exec
        op = operations.get(opName, None)

        if not op:
            continue

        # NOTE: now every operation function should have tick as the last parameter
        args.append(tick)
        op(*args)

        # TODO: after wait list implemented, increment of tick might be different
        tick+=1

def main():
    parser = argparse.ArgumentParser(description='Replicated Concurrency Control and Recovery.')
    parser.add_argument('testFile', nargs="?", default="tests/test3.txt", help='Test File')
    args = parser.parse_args()

    curDatetime = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
    logging.basicConfig(
        level= logging.INFO,
        format='%(levelname)s: [%(module)s] %(message)s',
        handlers=[
            logging.FileHandler(f"logs/{curDatetime}.log"),
            logging.StreamHandler()
        ]
    )

    DM = DataMgr(NUM_OF_SITES, NUM_OF_VARIABLES)
    TM = TransactionMgr(DM)

    execs = process_input(args.testFile)
    run(execs, DM, TM)
    

if __name__ == "__main__":
    main()