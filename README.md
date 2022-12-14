# Replicated Concurrency Control and Recovery 
## CSCI-GA 2423-001 (Advanced Database Systems, Fall 2022) Final Project

Created by *Tanran Zheng (tz408)* and *Daria Xu (xx2085)*.

Courant Institute of Mathematical Sciences - New York University


# Introduction
This project aims to implement a distributed database system, which has following features:  

(1) multiversion concurrency control;  
(2) deadlock detection (cycle detection with youngest victim selection);  
(3) replication (available copies algorithm using strict two phase locking);  
(4) failure recovery (under available copies algorithm).  

The program mainly consists of two parts: the data management part and the transaction management part. For further details of the design, please see [design_document.pdf](https://github.com/DariaXu/RepCRec/blob/main/Design%20Document.pdf).

# Setup

## Directory Structure

`./src`: directory contains source code.  
`./output`: directory contains outputs. It will be auto-created if once the program executes.
`./logs`: directory contains logging files. It will be auto-created if once the program executes.  
`./tests`: contains 48 test cases, test scripts (`run_all_tests.py`), and expected output (`./tests/correct_output`).  

## Modules

`main.py`: contains the main function to run the implementation (arg parser, read inputs, logging setup, etc.).  
`data_mgr.py`: contains the class that implements the data manager.  
`transaction_mgr.py`: contains the class that implements the transaction manager.  
`waitlist_mgr.py`: contains the implementation of the waitlist object.  
`Lock.py`: contains the implementation of the lock object.  
`Site.py`: contains the implementation of the site object.  
`utils.py`: contains utility functions. 
`const.py`: contains constants used in this project.  

`tests/run_all_tests.py`: script to run all test cases in directory _./tests/_ and compare the results to the expected results (in _./test/correct\_output_)



# TO RUN TESTS

**Input format**
Operations of transactions should be provided to the program in .txt file following the format given in the course syllabus.  

-----
**Run a single input test**
```
python3 src/main.py   \
--testFile <PathToTestFile>  \
--stdout  # remove this arg to not printing out the results
```
-----  
**Run all test cases in _./tests/_**
```
python3 tests/run_all_tests.py
```  

Note for `run_all_test.py`: 
- all test files should be placed in directory _./tests/_, named by `test<unique_numerical_number>.txt`. 
- **Modify** the 'TEST_FOLDER' variable in `run_all_tests.py` if test cases are stored in other directory path.
- By default, `--stdout` flag is not used.

-----  

# Notes

Authors are listed in the docstring at the begining of each module.