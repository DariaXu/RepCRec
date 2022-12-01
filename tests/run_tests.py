import os

numOfTest = 21
for test in range(1, numOfTest+1):
    testFile = "tests\\test"+str(test)+ ".txt"
    execution = "python3 script\\main.py "+ testFile
    cmd = os.system(execution)
    # exit_code = os.WEXITSTATUS(cmd)
    if cmd != 0:
        print(f"{testFile} Failed!!!!")