import Lock
from const import R_LOCK, RW_LOCK

class Site:
    def __init__(self, variables) -> None:
        self.lockTable = {}
        self.curWrites = {}
        self.curReads = {}
        self.committedVariables = {}
        for var in variables:
            self.committedVariables[var.name] = var
        
        self.isActive = False
        self.recoveredTime = 0

    def recover(self, tick):
        """
        Recover this site

        Parameters:
		    tick: The time this site is recover
        """

        self.isActive = True
        self.recoveredTime = tick

    def fail(self):
        """
        Fail this site, and notify all transactions that are accessing this site to abort,
        clear the curReads, the curWrites and the lock table.
        """
        self.isActive = False
        # TODO: notify notify all transactions that are accessing this site(curReads and curWrites) to abort

        self.curReads = {}
        self.curWrites = {}
        self.lockTable = {}
    
    def ifContains(self,x):
        return x in self.committedVariables

    def if_available_to_read(self, transaction, x, ifLockNeeded):
        """
        Check if variable x is ready to read.

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

        if ifLockNeeded and x in self.lockTable:
            lockObjs = self.lockTable[x]
            for lock in lockObjs:
                if lock.state == RW_LOCK and lock.transaction != transaction:
                    # write lock from other transaction
                    return False

        return True

    def if_available_to_write(self, transaction, x):
        """
        Check if variable x is ready to write.

        Parameters:
		    x: Variable name 
	    Returns: 
		    True if x can be write on this site; False otherwise
        """
        if x not in self.committedVariables:
            return False

        if x in self.lockTable:
            lockObjs = self.lockTable[x]
            for lock in lockObjs:
                if lock.transaction != transaction:
                    # read or write lock from other transaction
                    return False

        return True

    def lock_variable(self, transaction, x, lock_state):
        """
        Create a lock object and add it to the lock table. Remove read lock if necessary.

        Parameters:
		    transaction: transaction name
		    x: name of the variable to lock
		    lock_state: the type of lock
        """
        # if lock_state == R_LOCK:
        #     if not self._if_available_to_read(transaction, x):
        #         return False
        # elif lock_state == RW_LOCK:
        #     if not self._if_available_to_write(transaction, x):
        #         return False

        lockObj = Lock(lock_state, transaction)
        if x not in self.lockTable:
            self.lockTable[x] = [lockObj]
            return
        
        locks = self.lockTable[x]
        if lockObj in locks:
            # already locked
            return
        
        if lock_state == RW_LOCK:
            # remove read lock from the same transaction
            newLocks = [lock for lock in locks if not (lock.state == R_LOCK and lock.transaction == transaction)]
            self.lockTable[x] = newLocks

        # lock x
        self.lockTable[x].append(lockObj)

                
    
