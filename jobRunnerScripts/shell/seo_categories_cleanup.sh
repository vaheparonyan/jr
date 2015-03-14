echo $0 $1 
NUM_TO_KEEP=2
#DUMP_DIR="${HOME}/dump/categories"
DUMP_DIR=$1

echo "Cleaning up older parsed files"
cd ${DUMP_DIR}
FILES_TO_KEEP=(`ls | grep -v current | sort | tail -${NUM_TO_KEEP}`)
for i in `ls | grep -v current`; do
    PRESERVE=0
    for a in ${FILES_TO_KEEP[@]}; do
        if [ $i == $a ]; then
            PRESERVE=1
        fi;
    done;
    if [ ${PRESERVE} == 0 ]; then
        rm -rf $i
    fi;
done

echo "Done"
