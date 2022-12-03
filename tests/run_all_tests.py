import os

# NOTE: modify this if tests cases are stored in path different that pwd
TEST_FOLDER = "./tests"

def cmp_file(filename1, filename2):
    if open(filename1,'r').read() == open(filename2,'r').read():
        return True
    return False

if __name__ == "__main__":
    # test file name format: `test<unique_numerical_number>.txt`
    numOfTest = [f[4:-4] for f in os.listdir(TEST_FOLDER) if f.startswith('test') and f.endswith('.txt')]

    num_of_correct = 0
    for test in numOfTest:
        testFile = os.path.join("tests", f"test{str(test)}.txt")
        execution = f"python3 {os.path.join('src', 'main.py')} --testFile {testFile}"
        cmd = os.system(execution)
        if cmd != 0:
            print(f"{testFile} Failed!!!!")
        else:
            outFile = os.path.join('output', f'test{str(test)}.out')
            correctOut = os.path.join("tests", "correct_output", f"test{str(test)}.out")
            if not os.path.exists(outFile):
                print(f"{outFile} is not exist!!!")
                continue
            if not cmp_file(outFile, correctOut):
                print(f"{outFile} is not correct!!!")
            else:
                num_of_correct += 1
                
    print(f"Number of correct test case: {num_of_correct} out of {len(numOfTest)}.")

            