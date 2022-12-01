import logging

logger = logging.getLogger(__name__)

class WaitObj(object):
    def __init__(self, t, op, args, blockedBy) -> None:
        """
        Parameters:
            t: transaction object
            blockedBy: list of transaction object
        """
        self.operation = (op, args)
        self.t = t
        self.blockedBy = []
        for t in blockedBy:
            if t not in self.blockedBy:
                self.blockedBy.append(t)
    
    def __iter__(self):
        yield "operation", self.operation
        yield "t", self.t
        yield "blockedBy", self.blockedBy

    def __eq__(self, other): 
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __str__(self) -> str:
        return f"{self.operation[0]}({self.operation[1]})"

    def __repr__(self) -> str:
        return f"{self.operation[0]}({self.operation[1]})"
    
    
class WaitList(object):
    def __init__(self) -> None:
        self.waitList = []

        self.visitedWaitObj = []
        self.path = []

    def add_to_waitList(self, t, op, args, blockedBy):
        """
        Parameters:
            t: transaction object
        """
        waitObj = WaitObj(t, op, args, blockedBy)
        self.waitList.append(waitObj)
        logger.debug(f"Transaction {t} blocked by {waitObj.blockedBy}, added to wait list. {op}({args})")

    def notify_unblock(self, transaction):
        """
        Remove blocked lock object from block list.

        Parameters:
            transaction: transaction object
        """
        for waitObj in self.waitList:
            if transaction in waitObj.blockedBy:
                waitObj.blockedBy.remove(transaction)
    
    def if_waiting_on(self, transaction):
        for waitObj in self.waitList:
            if transaction in waitObj.blockedBy:
                return True
        return False

    def get_waitList(self):
        return self.waitList

    def remove_from_waitList(self, waitObj):
        if waitObj in self.waitList:
            self.waitList.remove(waitObj)
    
    def remove_last_from_waitList(self):
        self.waitList.pop()

    def remove_waitObj_of_t(self, t):
        for waitObj in list(self.waitList):
            if waitObj.t == t:
                self.waitList.remove(waitObj)

    def deadlock_detection(self):
        self.visitedWaitObj = []
        youngest = []
        for waitObj in self.waitList:
            if waitObj in self.visitedWaitObj:
                continue
            self.path = []
            if self._if_deadlock(waitObj.t, [waitObj.t]):
                logger.debug(f"Deadlock Detected!")
                youngest.append(self.get_youngest_transaction())
                
        if not youngest:
            logger.debug(f"No Deadlock Detected!")
        return youngest

    def _if_deadlock(self, waitingT, path):
        waitObj = self._get_waitObj_by_tObj(waitingT)
        if not waitObj:
            # this transaction is not blocked
            return False

        self.visitedWaitObj.append(waitObj)
        for waitFor in waitObj.blockedBy:
            if waitFor in path:
                self.path = path
                return True

            if self._if_deadlock(waitFor, path+[waitFor]):
                return True

        return False          

    def get_youngest_transaction(self):
        minTime = self.path[0].startTime
        youngest = self.path[0]
        for t in self.path:
             if t.startTime < minTime:
                minTime = t.startTime
                youngest = t

        logger.debug(f"Youngest transaction: {youngest}")
        return youngest

    def _get_waitObj_by_tObj(self, t):
        for waitObj in self.waitList:
            if t == waitObj.t:
                return waitObj
        return None