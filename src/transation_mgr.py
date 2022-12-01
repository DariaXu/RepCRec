from const import OperationType, ResultType
from waitlist_mgr import WaitList, WaitObj
import logging

logger = logging.getLogger(__name__)

class Transaction(object):
    def __init__(self, name, startTime, readOnly) -> None:
        """
        Initialize Transaction.

        Parameters
        -----------
        name: str 
            Transaction Name.
        startTime: int 
            Transaction start time.
        readOnly: bool
            If this transaction is read only transaction.
        """
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

        Parameters
        -----------
        t: str
            Transaction name
        """
        self.transactions[t] = Transaction(t, tick, readOnly=False)
        logger.debug(f"{tick}: Start transaction {t}")

    def start_RO_transaction(self, t, tick):
        """
        Create a RO transaction object, initialize it with the transaction name and current time as the start time.

        Parameters
        -----------
        t: str
            Transaction name
        """
        self.transactions[t] = Transaction(t, tick, readOnly=True)
        for site in self.dataMgr.get_available_sites():
            site.request_copy_for_RO(self.transactions[t])

        logger.debug(f"{tick}: Start RO transaction {t}")

    def read(self, t, x, tick):
        """
        Process read request.

        Parameters
        -----------
        t: str
            Transaction name
        x: str
            Name of the variable to write on
        tick: int
            Current tick

        Return: ResultType Enum
        -----------
        Type of return
        """
        if t not in self.transactions:
            # abort because of deadlock
            logger.debug(f"{t} skip end because aborted due to deadlock")
            return ResultType.STOP

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
            logger.info(f"{t} reads - "+ str(var))

        else:
            ifSuccess, var = self.dataMgr.request_read(transaction, x, tick)
            if not ifSuccess:
                # add to wait list, var is the lock object blocking current transaction
                self.waitLists.add_to_waitList(transaction, OperationType.READ, [t,x], blockedBy=var)
                return ResultType.WL

            logger.debug(f"{tick}: {t} successfully read {var}")
            logger.info(f"{t} reads - "+ str(var))

        return ResultType.SUCCESS

    def write(self, t, x, val ,tick):
        """
        Process write request.

        Parameters
        -----------
        t: str
            Transaction name
        x: str
            Name of the variable to write on
        val: str
            The value to write
        tick: int
            Current tick

        Return: ResultType Enum
        -----------
        Type of return
        """
        if t not in self.transactions:
            # abort because of deadlock
            logger.debug(f"{t} skip `end` because aborted due to deadlock")
            return ResultType.STOP
            
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
        -----------
        t: str
            Transaction object
        tick: int
            Current tick
        """
        logger.debug(f"{tick}: Receive request to abort {t}.")
        self.dataMgr.abort_on_all_sites(t)
        self.transactions.pop(t.name)
        self.waitLists.remove_waitObj_of_t(t)
        logger.info(f"Abort: {t.name}")

    def commit(self, t, tick):
        """
        Commit transaction
        
        Parameters:
        -----------
        t: str
            Transaction object
        tick: int
            Current tick
        """
        logger.debug(f"{tick}: Receive request to commit {t}.")
        self.dataMgr.commit_on_all_sites(t, tick)
        self.transactions.pop(t.name)

        if self.waitLists.get_waitObj_of_t(t):
            logger.error(f"{tick}: {t} There are pending executions, please check!")

        logger.info(f"Commit: {t.name}")

    def end(self, t, tick):
        """
        End Transaction
        
        Parameters:
        -----------
        t: str
            Transaction name
        tick: int
            Current tick
        """
        if t not in self.transactions:
            # abort because of deadlock
            logger.debug(f"{t} skips `end` because aborted due to deadlock")
            return 
        else:
            transaction = self.transactions[t]
        if transaction.abort:
            self.abort(transaction, tick)
        else:
            self.commit(transaction, tick)
        
    def dump(self):
        self.dataMgr.dump_all_sites()
