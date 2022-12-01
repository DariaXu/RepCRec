import os
import filecmp
from os.path import exists

numOfTest = [i for i in range(1,4)]+[3.5, 3.7] + [i for i in range(4,22)]

for test in numOfTest:
    testFile = "tests\\test"+str(test)+ ".txt"
    execution = "python3 src\\main.py "+ testFile
    cmd = os.system(execution)
    # exit_code = os.WEXITSTATUS(cmd)
    if cmd != 0:
        print(f"{testFile} Failed!!!!")
    else:
        outFile = "output\\"+str(test)+ ".out"
        correctOut ="tests\\correct_output\\"+str(test)+ ".out"

        if exists(correctOut) and not filecmp.cmp(outFile, correctOut):
            print(f"{outFile} is not correct!!!")

    