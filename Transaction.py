class Transaction(object):
    def __init__(self, name, startTime, readOnly) -> None:
        self.name = name
        self.startTime = startTime
        self.readOnly = readOnly
        self.abort = False