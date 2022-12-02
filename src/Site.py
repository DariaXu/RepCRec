from Lock import Lock
from const import LockState
import logging

logger = logging.getLogger(__name__)

class Variable:
    def __init__(self, name, value, onSite=None) -> None:
        """
        Initialize the Variable.

        Parameters
        -----------
        name: str 
            Variable Name.
        value: str 
            Variable value.
        onSite: int
            If this variable is not a replicated variable, onSite should be None; 
            otherwise, onSite will be the site number where this copy is on 
        """

        self.name = name
        self.value = value
        self.lastCommittedTime = -1
        self.onSite = onSite

    def __str__(self) -> str:
        return f"{self.name}: {self.value}"

    def __repr__(self) -> str:
        return f"{self.name}: {self.value}"

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
        
        self.copies = {}
        self.liningUp = {}
        self.isActive = False
        self.recoveredTime = -1

    def __repr__(self) -> str:
        return self.name

    def ifContains(self,x):
        return x in self.committedVariables

    def request_copy_for_RO(self, transaction):
        """
        Store copy of variables.

        Parameters
        -----------
        transaction: Transaction Object
        """
        if transaction.name not in self.copies:
            self.copies[transaction.name] = {}
        self.copies[transaction.name] = self.committedVariables.copy()

        logger.debug(f"Stored copy for RO transaction {transaction}")

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

        # if self.recoveredTime > transaction.startTime:
        #     return False

        if self.committedVariables[x].lastCommittedTime < self.recoveredTime:
            return False

        return True

    def if_available_to_read_only(self, transaction, x):
        """
        Check if variable x is ready to read only. 

        Parameters
        -----------
        transaction: Transaction Object
		x: str
            Variable name 
            
	    Returns: bool
        -----------
		True if x can be read only on this site; False otherwise
        """
        if self.recoveredTime > transaction.startTime:
            return False

        return True

    def lock_lining_up(self, transaction, x):
        """
        Store the information which transaction is waiting for a lock to release 

        Parameters
        -----------
        transaction: Transaction Object
		x: str
            Variable name 
        """
        if x not in self.liningUp:
            self.liningUp[x] = []

        self.liningUp[x].append(transaction)

    def remove_from_lock_lineup(self, transaction):
        """
        Remove from the line

        Parameters
        -----------
        transaction: Transaction Object
        """
        for var, ts in self.liningUp.items():
            if transaction in ts:
                ts.remove(transaction)
            self.liningUp[var] = ts


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
                # write lock by other transaction
                ifWLock = lock.state == LockState.RW_LOCK and lock.transaction != transaction
                # some transaction is currently waiting for this lock to release
                ifLockWaitUp = lock.state == LockState.R_LOCK and lock.linedUp
                
                if ifWLock or ifLockWaitUp:
                    # write lock from other transaction
                    lock.linedUp = True
                    blockedBy.append(lock.transaction)

        # no lock on this variable on current site, 
        # but some transaction is waiting to acquiring a write lock, 
        # because there is a read lock on this variable on other site
        if x in self.liningUp:
            blockedBy+=self.liningUp[x]

        return blockedBy

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

        lockObj = Lock(lock_state, transaction)
        if x not in self.lockTable:
            self.lockTable[x] = [lockObj]
            logger.debug(f"Site {self.name} - {transaction.name} successfully locked {x} with {lock_state}.")
            return blockedBy
        
        locks = self.lockTable[x]
        if lockObj in locks:
            logger.debug(f"Site {self.name} - {transaction.name} already locked {x} with {lock_state}.")
            return blockedBy

        if lock_state == LockState.R_LOCK:
            tempLockObj = Lock(LockState.RW_LOCK, transaction)
            if tempLockObj in locks:
                logger.debug(f"Site {self.name} - {transaction.name} already locked {x} with {LockState.RW_LOCK}.")
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

    def read_only(self, transaction, x):
        """
        Read only x. 

        Parameters
        -----------
        transaction: transaction object
        x: str
            Variable name 

        Returns
        -----------
        The value of x; None if read failed.
        """
        logger.debug(f"Site {self.name} - {transaction.name} read only {x}.")
        if transaction.name not in self.copies:
            logger.error(f"Site {self.name} - Failed to read only: There is no copy for {transaction.name}!! {self.copies}")
            return None

        varCopy = self.copies[transaction.name]

        if x not in varCopy:
            logger.error(f"Site {self.name} - Failed to read only: {x} is not in the copy!! {varCopy}")
            return None

        # if transaction not in self.curReads:
        #     self.curReads.append(transaction)

        return varCopy[x]

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

        self.curWrites[transaction][x] = Variable(x, val)

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

        if transaction.name in self.copies:
            self.copies.pop(transaction.name)

        self.remove_from_lock_lineup(transaction)

        for x, lockObjs in self.lockTable.items():
            newLockLst = [lock for lock in lockObjs if lock.transaction != transaction]
            self.lockTable[x] = newLockLst

        logger.debug(f"Site {self.name} - {transaction.name} aborted.")

    def commit(self, transaction, tick):
        """
        Transaction commit. 

        Parameters
        -----------
        transaction: transaction object
        tick: int
            Current tick
        """
        
        if transaction in self.curReads:
            self.curReads.remove(transaction)

        if transaction in self.curWrites:
            allWritesDict = self.curWrites[transaction]
            for x, v in allWritesDict.items():
                v.lastCommittedTime = tick
                self.committedVariables[x] = v

            self.curWrites.pop(transaction)

        if transaction.name in self.copies:
            self.copies.pop(transaction.name)

        self.remove_from_lock_lineup(transaction)

        for x, lockObjs in self.lockTable.items():
            newLockLst = [lock for lock in lockObjs if lock.transaction != transaction]
            self.lockTable[x] = newLockLst

        logger.debug(f"Site {self.name} - {transaction.name} committed.")
            