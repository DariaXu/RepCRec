"""Script that contains the class that implements the waitlist manager.

@Author: Tanran Zheng (tz408@nyu.edu) and Daria Xu (xx2085@nyu.edu).
@Date: Dec/03/2022
@Instructor: Prof. Dennis Shasha

"""

import logging

logger = logging.getLogger(__name__)

class WaitObj(object):
    def __init__(self, t, op, args, blockedBy) -> None:
        """
        Initialize Wait Object.

        Parameters
        -----------
        t: transaction object 
        op: OperationType Enum  
            Operation read or write
        args: list
            List of operation arguments
        blockedBy: list
            list of transaction object
        """
        self.operation = (op, args)
        self.t = t
        self.blockedBy = []
        for t in blockedBy:
            if t not in self.blockedBy:
                self.blockedBy.append(t)

    def __eq__(self, other): 
        return isinstance(other, type(self)) and \
            self.operation == other.operation and self.t == other.t
                # self.blockedBy == other.blockedBy

    def __str__(self) -> str:
        return f"{self.operation[0]}({self.operation[1]})"

    def __repr__(self) -> str:
        return f"{self.operation[0]}({self.operation[1]})"
    
    
class WaitList(object):
    def __init__(self) -> None:
        self.waitList = []

        self.visitedWaitObj = []
        self.path = []

    def get_waitList(self):
        return self.waitList

    def get_waitObj_of_t(self, t):
        """
        Get wait object from transaction t

        Parameters
        -----------
        t: transaction object 

        Return: Wait Obj
        """
        for waitObj in self.waitList:
            if t == waitObj.t:
                return waitObj
        return None

    def remove_from_waitList(self, waitObj):
        if waitObj in self.waitList:
            self.waitList.remove(waitObj)
    
    # def remove_last_from_waitList(self):
    #     self.waitList.pop()

    def remove_waitObj_of_t(self, t):
        """ remove any wait object from transaction t"""
        for waitObj in list(self.waitList):
            if waitObj.t == t:
                self.waitList.remove(waitObj)

    def add_to_waitList(self, t, op, args, blockedBy):
        """
        Add transaction to wait list

        Parameters
        -----------
        t: transaction object
        op: OperationType Enum  
            Operation read or write
        args: list
            List of operation arguments 
        blockedBy: list
            list of transaction object
        """
        waitObj = WaitObj(t, op, args, blockedBy)
        if waitObj in self.waitList:
            return
        self.waitList.append(waitObj)
        t.isBlocked = True
        logger.debug(f"Transaction {t} blocked by {waitObj.blockedBy}, added to wait list. {op}({args})")

        if blockedBy:
            logger.info(f"Transaction {t.name} blocked by a lock conflict. Locks: {sorted(list(set(blockedBy)))}")
        else:
            logger.info(f"Transaction {t.name} blocked because site is down.")

    # def notify_unblock(self, transaction):
    #     """
    #     Remove blocked lock object from block list.

    #     Parameters:
    #         transaction: transaction object
    #     """
    #     for waitObj in self.waitList:
    #         if transaction in waitObj.blockedBy:
    #             waitObj.blockedBy.remove(transaction)
    
    # def if_waiting_on(self, transaction):
    #     for waitObj in self.waitList:
    #         if transaction in waitObj.blockedBy:
    #             return True
    #     return False

    def deadlock_detection(self):
        """
        Deadlock detection

        Return list
        -----------
        If there is a deadlock, return the youngest transaction to abort. 
        Otherwise, return an empty list.
        """
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
        return set(youngest)

    def _if_deadlock(self, waitingT, path):
        waitObj = self.get_waitObj_of_t(waitingT)
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
        maxTime = -1
        youngest = None
        for t in self.path:
             if t.startTime > maxTime:
                maxTime = t.startTime
                youngest = t

        logger.debug(f"Youngest transaction: {youngest}")
        return youngest
