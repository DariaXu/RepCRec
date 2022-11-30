class Transaction(object):
    def __init__(self, name, startTime, readOnly) -> None:
        self.name = name
        self.startTime = startTime
        self.readOnly = readOnly

        self.isBlocked = False
        self.abort = False

    def __eq__(self, other): 
        if not isinstance(other, Transaction):
            return False

        return self.name == other.name and self.startTime == other.startTime and self.readOnly == other.readOnly