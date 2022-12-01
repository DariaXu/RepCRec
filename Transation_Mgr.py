from const import OperationType, ResultType
from Waitlist_Mgr import WaitList, WaitObj
import logging

logger = logging.getLogger(__name__)

class Transaction(object):
    def __init__(self, name, startTime, readOnly) -> None:
        self.name = name
        self.startTime = startTime
        self.readOnly = readOnly

        self.isBlocked = False
        self.abort = False

    def __iter__(self):
        yield "name", self.name
        yield "startTime", self.startTime
        yield "readOnly", self.readOnly
        yield "isBlocked", self.isBlocked
        yield "abort", self.abort

    def __eq__(self, other): 
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __hash__(self):
        return hash(tuple(self))

    def __str__(self) -> str:
        return f"{self.startTime}-{self.name}"

    def __repr__(self) -> str:
        return f"{self.startTime}-{self.name}"
    
class TransactionMgr(object):
    def __init__(self, dataMgr) -> None:
        self.dataMgr = dataMgr
        self.transactions = {}

        self.waitLists = WaitList()

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
        logger.debug(f"{tick}: {t} tries to read {x}...")

        transaction = self.transactions[t]
        if transaction.abort:
            logger.debug(f"{tick}: {t} aborted, will not process read")
            return ResultType.STOP

        if transaction.readOnly:
            var = self.dataMgr.request_read_only(transaction, x)
            if var == None:
                if self.dataMgr.get_site_index(x) == None:
                    # replicated variable
                    self.abort(t, tick)
                    return ResultType.ABORT
                else:
                    self.waitLists.add_to_waitList(transaction, OperationType.READ, [t,x], blockedBy=None)
                    return ResultType.WL

            logger.debug(f"{tick}: {t} successfully read {var}")
            logger.info(str(var))

        else:
            ifSuccess, var = self.dataMgr.request_read(transaction, x, tick)
            if not ifSuccess:
                # add to wait list, var is the lock object blocking current transaction
                self.waitLists.add_to_waitList(transaction, OperationType.READ, [t,x], blockedBy=var)
                return ResultType.WL

            logger.debug(f"{tick}: {t} successfully read {var}")
            logger.info(str(var))

        return ResultType.SUCCESS

    def write(self, t, x, val ,tick):
        """
        Process write request.

        Parameters:
            t: transaction name
            x: name of the variable to write on
            val: the value to write
            tick: current tick
        """
        logger.debug(f"{tick}: {t} tries to write {x}: {val}...")

        transaction = self.transactions[t]
        if transaction.abort:
            logger.debug(f"{tick}: {t} aborted, will not process write")
            return ResultType.STOP

        ifSuccess, var = self.dataMgr.request_write(transaction, x, val, tick)
        if not ifSuccess:       
            self.waitLists.add_to_waitList(transaction, OperationType.WRITE, [t,x,val], blockedBy=var)
            return ResultType.WL

        logger.debug(f"{tick}: {t} successfully write {x}: {val}")
        return ResultType.SUCCESS
            
    def abort(self, t, tick):
        """
        Process abort request.
        
        Parameters:
            t: transaction object
            tick: current tick
        """
        logger.debug(f"{tick}: Receive request to abort {t}.")
        self.dataMgr.abort_on_all_sites(t)
        self.transactions.pop(t.name)
        self.waitLists.remove_waitObj_of_t(t)

    # def get_next_waitList(self):
    #     """
    #     Get next transaction in the wait list.
        
    #     Return:
    #         Return next in line operation with parameters; if no transaction is waiting, return None.
    #     """
    #     if not self.waitLists:
    #         return None

    #     nextOp, args = self.waitLists[0]
    #     self.waitLists.pop(0)

    #     return nextOp, args

    # def add_to_waitList(self, t, op, args, blockedBy):
    #     """
    #     Add operation to wait list.
        
    #     Parameters:
    #         t: transaction name
    #         op: operation
    #         args: arguments list
    #         blockedBy: list of transactions blocking t
    #     """
    #     self.transactions[t].isBlocked = True
        
