from Data_Mgr import DataMgr
from Transation_Mgr import TransactionMgr

NUM_OF_VARIABLES = 20
NUM_OF_SITES = 10

def main():
    dataMgr = DataMgr(NUM_OF_SITES, NUM_OF_VARIABLES)
    transMgr = TransactionMgr(dataMgr)

    

if __name__ == "main":
    main()