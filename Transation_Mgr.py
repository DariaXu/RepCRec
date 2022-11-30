from Transaction import Transaction
from const import READ, WRITE
import logging

logger = logging.getLogger(__name__)

class TransactionMgr(object):
    def __init__(self, dataMgr) -> None:
        self.dataMgr = dataMgr
        self.transactions = {}

        self.waitLists = []

    def start_transaction(self, t, tick):
        """
        Create a transaction object, initialize it with the transaction name and current time as the start time.

        Parameters:
            t: transaction name
        """
        self.transactions[t] = Transaction(t, tick, readOnly=False)
        logger.debug(f"{tick}: Start transaction {t}")

    def start_RO_transaction(self, t, tick):
        """
        Create a RO transaction object, initialize it with the transaction name and current time as the start time.

        Parameters:
            t: transaction name
        """
        self.transactions[t] = Transaction(t, tick, readOnly=True)
        logger.debug(f"{tick}: Start RO transaction {t}")

    def read(self, t, x, tick):
        """
        Process read request.

        Parameters:
            t: transaction name
            x: name of the variable to write on
            tick: current tick
        """
        transaction = self.transactions[t]
        if transaction.readOnly:
            var = self.dataMgr.request_read_only(transaction, x)
            if not var:
                self.abort(t, tick)
                return

            logger.debug(f"{tick}: {t} successfully read {var}")
            logger.info(str(var))

        else:
            var = self.dataMgr.request_read(transaction, x)
            if not var:
                # add to wait list
                self.add_to_waitList(t, READ, [t,x])
                return

            logger.debug(f"{tick}: {t} successfully read {var}")
            logger.info(str(var))
            

    def abort(self, t, tick):
        """
        Process abort request.
        
        Parameters:
            t: transaction name
            tick: current tick
        """
        self.transactions[t].abort = True
        logger.debug(f"{tick}: Receive request to abort {t}.")

    def get_next_waitList(self):
        """
        Get next transaction in the wait list.
        
        Return:
            Return next in line operation with parameters; if no transaction is waiting, return None.
        """
        if not self.waitLists:
            return None

        nextOp, args = self.waitLists[0]
        self.waitLists.pop(0)

        return nextOp, args

    def add_to_waitList(self, t, op, args):
        """
        Add operation to wait list.
        
        Parameters:
            t: transaction name
            op: operation
            args: arguments list
        """
        self.transactions[t].isBlocked = True
        self.waitLists.append((op, args))
        logger.debug(f"Transaction {t} added to wait list. {op}({args})")
