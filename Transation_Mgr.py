import Transaction
class TransactionMgr(object):
    def __init__(self, dataMgr) -> None:
        self.dataMgr = dataMgr
        self.transactions = {}

    def start_transaction(self, t, tick, ifReadOnly):
        """
        Create a transaction object, initialize it with the transaction name and current time as the start time.

        Parameters:
            t: transaction name
        """
        self.transactions[t] = Transaction(t, tick, ifReadOnly)


