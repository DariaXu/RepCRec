"""
Script that contains the implementation of the lock object.

@Author: Daria Xu (xx2085@nyu.edu).
@Date: Dec/03/2022
@Instructor: Prof. Dennis Shasha

"""

class Lock(object):
    def __init__(self, state, transaction) -> None:
        self.state = state
        # transaction object
        self.transaction = transaction
        # self.lockedTime = tick
        self.linedUp = False

    def __iter__(self):
        yield "state", self.state
        yield "transaction", self.transaction
        yield "linedUp", self.linedUp

    def __eq__(self, other): 
        return isinstance(other, type(self)) \
            and self.state == other.state and self.transaction == other.transaction