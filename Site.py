from Lock import Lock
from const import LockState
import logging

logger = logging.getLogger(__name__)

class Site:
    def __init__(self, name, variables) -> None:
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

    def recover(self, tick):
        """
        Recover this site

        Parameters:
		    tick: The time this site is recover
        """

        self.isActive = True
        self.recoveredTime = tick
        # logger.info(f"{tick}: Recovered Site.")

    def fail(self, tick):
        """
        Fail this site, and notify all transactions that are accessing this site to abort,
        clear the curReads, the curWrites and the lock table
        .
        Parameters:
		    tick: The time this site fails
        """
        self.isActive = False
        # TODO: notify notify all transactions that are accessing this site(curReads and curWrites) to abort
        for t in self.curReads:
            t.abort = True

        for t in self.curWrites.keys():
            t.abort = True

        self.curReads = []
        self.curWrites = {}
        self.lockTable = {}
        # logger.info(f"{tick}: Failed Site.")
    
    def ifContains(self,x):
        return x in self.committedVariables

    def if_available_to_read(self, transaction, x):
        """
        Check if variable x is ready to read. 
        If lock is needed, check if x can be lock with read lock.

        Parameters:
		    x: Variable name 
            transaction: transaction object
	    Returns: 
		    True if x can be read on this site; False otherwise
        """
        if x not in self.committedVariables:
            return False

        if self.recoveredTime > transaction.startTime:
            return False

        if self.committedVariables[x].lastCommittedTime < self.recoveredTime:
            return False

        return True

    def get_r_lock_block(self,transaction, x):
        blockedBy = []
        if x in self.lockTable:
            lockObjs = self.lockTable[x]
            for lock in lockObjs:
                if lock.state == LockState.RW_LOCK and lock.transaction != transaction:
                    # write lock from other transaction
                    lock.linedUp = True
                    blockedBy.append(lock.transaction)
        return blockedBy

    def if_available_to_write(self, transaction, x):
        """
        Check if variable x is ready to write. 
        Check if x can be lock with write lock.

        Parameters:
            transaction: transaction object
		    x: Variable name 
	    Returns: 
		    True if x can be write on this site; False otherwise
        """
        if x not in self.committedVariables:
            return False

        # if x in self.lockTable:
        #     lockObjs = self.lockTable[x]
        #     for lock in lockObjs:
        #         if lock.transaction != transaction:
        #             # read or write lock from other transaction
        #             return False

        return True

    def get_rw_lock_block(self,transaction, x):
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
        Create a lock object and add it to the lock table. Remove read lock if necessary.

        Parameters:
		    transaction: transaction object
		    x: name of the variable to lock
		    lock_state: the type of lock
            tick: current tick
        """

        # check if can lock
        blockedBy = []
        if lock_state == LockState.R_LOCK:
            blockedBy = self.get_r_lock_block(transaction, x)
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

    def read(self, transaction, x):
        """
        Read x. 

        Parameters:
            transaction: transaction object
            x: name of the variable to read
        Returns:
            The value of x
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

        Parameters:
            transaction: transaction object
            x: name of the variable to write
            val: the value to write
        """
        logger.debug(f"Site {self.name} - {transaction.name} write {x}={val}.")
        if transaction not in self.curWrites:
            self.curWrites[transaction] = {}

        self.curWrites[transaction][x] = val

    def abort(self, transaction):
        """
        Transaction abort. 

        Parameters:
            transaction: transaction object
        """
        logger.debug(f"Site {self.name} - {transaction.name} abort.")
        if transaction in self.curReads:
            self.curReads.remove(transaction)
        if transaction in self.curWrites:
            self.curWrites.pop(transaction)

        for x, lockObjs in self.lockTable.items():
            newLockLst = [lock for lock in lockObjs if lock.transaction != transaction]
            self.lockTable[x] = newLockLst