from Lock import Lock
from const import LockState
import logging

logger = logging.getLogger(__name__)

class Site:
    def __init__(self, name, variables) -> None:
        """
        Initialize the site.

        Parameters
        -----------
        name: str 
            Site Name.
        variables: list 
            List of Variable object for initialization
        """

        self.name = name
        self.lockTable = {}
        self.curWrites = {}
        self.curReads = []
        self.committedVariables = {}
        for var in variables:
            self.committedVariables[var.name] = var
        
        self.isActive = False
        self.recoveredTime = -1

    def __repr__(self) -> str:
        return self.name

    def ifContains(self,x):
        return x in self.committedVariables

    def if_available_to_read(self, transaction, x):
        """
        Check if variable x is ready to read. 

        Parameters
        -----------
        transaction: Transaction Object
		x: str
            Variable name 
            
	    Returns: bool
        -----------
		True if x can be read on this site; False otherwise
        """
        if x not in self.committedVariables:
            return False

        if self.recoveredTime > transaction.startTime:
            return False

        if self.committedVariables[x].lastCommittedTime < self.recoveredTime:
            return False

        return True

    def _get_r_lock_block(self, transaction, x):
        """
        Check whether read lock can be acquire.

        Parameters
        -----------
        transaction: Transaction Object
		x: str
            Variable name 
            
	    Returns: list
        -----------
		List of transaction object that is blocking this read lock; 
        return empty list if read lock can be acquire.
        """

        blockedBy = []
        if x in self.lockTable:
            lockObjs = self.lockTable[x]
            for lock in lockObjs:
                if lock.state == LockState.RW_LOCK and lock.transaction != transaction:
                    # write lock from other transaction
                    lock.linedUp = True
                    blockedBy.append(lock.transaction)
        return blockedBy

    # def _if_available_to_write(self, transaction, x):
    #     """
    #     Check if variable x is ready to write. 

    #     Parameters:
    #         transaction: transaction object
	# 	    x: Variable name 
	#     Returns: 
	# 	    True if x can be write on this site; False otherwise
    #     """
    #     if x not in self.committedVariables:
    #         return False

    #     return True

    def get_rw_lock_block(self, transaction, x):
        """
        Check whether write lock can be acquire.

        Parameters
        -----------
        transaction: Transaction Object
		x: str
            Variable name 
            
	    Returns: list
        -----------
		List of transaction object that is blocking this write lock; 
        return empty list if read lock can be acquire.
        """
        blockedBy = []
        if x in self.lockTable:
            lockObjs = self.lockTable[x]
            for lock in lockObjs:
                if lock.transaction != transaction or (lock.state == LockState.R_LOCK and lock.linedUp):
                    # read or write lock from other transaction
                    lock.linedUp = True
                    blockedBy.append(lock.transaction)
        return blockedBy

    def lock_variable(self, transaction, x, lock_state, tick):
        """
        Check if lock can be acquire, then lock. Remove read lock if necessary.

        Parameters
        -----------
        transaction: transaction object
        x: str
            Variable name 
        lock_state: LockState Enum 
            The type of lock
        tick: int 
            Current tick

        Returns: list
        -----------
		List of transaction object that is blocking the lock; 
        return empty list if lock successfully.
        """

        # check if can lock
        blockedBy = []
        if lock_state == LockState.R_LOCK:
            blockedBy = self._get_r_lock_block(transaction, x)
        else:
            blockedBy = self.get_rw_lock_block(transaction, x)

        if blockedBy:
            return blockedBy

        lockObj = Lock(lock_state, transaction, tick)
        if x not in self.lockTable:
            self.lockTable[x] = [lockObj]
            logger.debug(f"Site {self.name} - {transaction.name} successfully locked {x} with {lock_state}.")
            return blockedBy
        
        locks = self.lockTable[x]
        if lockObj in locks:
            logger.debug(f"Site {self.name} - {transaction.name} already locked {x} with {lock_state}.")
            return blockedBy
        
        if lock_state == LockState.RW_LOCK:
            # remove read lock from the same transaction
            newLocks = [lock for lock in locks if not (lock.state == LockState.R_LOCK and lock.transaction == transaction)]
            self.lockTable[x] = newLocks
            logger.debug(f"Site {self.name} - {transaction.name} removed {LockState.R_LOCK} on {x}.")

        # lock x
        self.lockTable[x].append(lockObj)
        logger.debug(f"Site {self.name} - {transaction.name} successfully locked {x} with {lock_state}.")
        return blockedBy

    def recover(self, tick):
        """
        Recover this site

        Parameters:
        -----------
		tick: int
            The time this site is recover
        """

        self.isActive = True
        self.recoveredTime = tick
        # logger.info(f"{tick}: Recovered Site.")

    def fail(self, tick):
        """
        Fail this site, and notify all transactions that are accessing this site to abort,
        clear the curReads, the curWrites and the lock table
        
        Parameters
        -----------
		tick: int
            The time this site fails
        """
        self.isActive = False
        for t in self.curReads:
            t.abort = True

        for t in self.curWrites.keys():
            t.abort = True

        self.curReads = []
        self.curWrites = {}
        self.lockTable = {}
        # logger.info(f"{tick}: Failed Site.")

    def read(self, transaction, x):
        """
        Read x. 

        Parameters
        -----------
        transaction: transaction object
        x: str
            Variable name 

        Returns
        -----------
        The value of x; None if read failed.
        """
        # t = transaction.name
        logger.debug(f"Site {self.name} - {transaction.name} read {x}.")
        if transaction in self.curWrites and x in self.curWrites[transaction]:
            # when t previously wrote to x but not committed yet
            return self.curWrites[transaction][x]

        if x not in self.committedVariables:
            logger.error(f"Site {self.name} - Failed to read: {x} is not in committed variable!! {self.committedVariables}")
            return None

        if transaction not in self.curReads:
            self.curReads.append(transaction)

        return self.committedVariables[x]

    def write(self, transaction, x, val):
        """
        Write x. 

        Parameters
        -----------
        transaction: transaction object
        x: str
            Variable name 
        val: str
            The value to write
        """
        logger.debug(f"Site {self.name} - {transaction.name} write {x}={val}")
        if transaction not in self.curWrites:
            self.curWrites[transaction] = {}

        self.curWrites[transaction][x] = val

        logger.info(f"Site {self.name}: {transaction.name} write {x}={val}")

    def abort(self, transaction):
        """
        Transaction abort. 

        Parameters
        -----------
        transaction: transaction object
        """
        
        if transaction in self.curReads:
            self.curReads.remove(transaction)
        if transaction in self.curWrites:
            self.curWrites.pop(transaction)

        for x, lockObjs in self.lockTable.items():
            newLockLst = [lock for lock in lockObjs if lock.transaction != transaction]
            self.lockTable[x] = newLockLst

        logger.debug(f"Site {self.name} - {transaction.name} aborted.")

    def commit(self, transaction):
        """
        Transaction commit. 

        Parameters
        -----------
        transaction: transaction object
        """
        
        if transaction in self.curReads:
            self.curReads.remove(transaction)
        if transaction in self.curWrites:
            allWritesDict = self.curWrites[transaction]
            for x, v in allWritesDict.items():
                self.committedVariables[x].value = v

            self.curWrites.pop(transaction)

        for x, lockObjs in self.lockTable.items():
            newLockLst = [lock for lock in lockObjs if lock.transaction != transaction]
            self.lockTable[x] = newLockLst

        logger.debug(f"Site {self.name} - {transaction.name} committed.")
            