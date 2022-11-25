class Lock(object):
    def __init__(self, state, transaction) -> None:
        self.state = state
        self.transaction = transaction 

    def __eq__(self, other): 
        if not isinstance(other, Lock):
            return False

        return self.state == other.state and self.transaction == other.transaction