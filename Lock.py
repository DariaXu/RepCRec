class Lock(object):
    def __init__(self, state, transaction, tick) -> None:
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
        return isinstance(other, type(self)) and tuple(self) == tuple(other)