from Transaction import Transaction
import logging

logger = logging.getLogger(__name__)

class TransactionMgr(object):
    def __init__(self, dataMgr) -> None:
        self.dataMgr = dataMgr
        self.transactions = {}

    def start_transaction(self, t, tick):
        """
        Create a transaction object, initialize it with the transaction name and current time as the start time.

        Parameters:
            t: transaction name
        """
        self.transactions[t] = Transaction(t, tick, readOnly=False)
        logger.info(f"{tick}: Start transaction {t}")

    def start_RO_transaction(self, t, tick):
        """
        Create a RO transaction object, initialize it with the transaction name and current time as the start time.

        Parameters:
            t: transaction name
        """
        self.transactions[t] = Transaction(t, tick, readOnly=True)
        logger.info(f"{tick}: Start RO transaction {t}")


