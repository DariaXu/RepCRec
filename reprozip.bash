## reprozip command example on cims
## note: change the chmod of this bash first `chmod +x reprozip.bash`

chmod +x reprozip
chmod +x reprounzip

./reprozip trace python3 src/main.py tests/test1.txt
## for shell script run: ./reprozip trace <script>.sh
## run ls -al, you should be able to see a file “.reprozip-trace”
## If you get any error during this step, run `export LC_ALL=C`

./reprozip pack res_repro_file
## my_repro_file.rpz will be created.
## This is the file you will upload for the project!!