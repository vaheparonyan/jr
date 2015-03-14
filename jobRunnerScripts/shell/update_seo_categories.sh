echo $0 $1 $2 $3 $4 $5 $6 $7 $8


#DB_HOST="tdwc"
DB_HOST=$1
#DB_USER="vparonyan"
DB_USER=$2
#DB_PASSWORD="LetsDoTDWC12"
DB_PASSWORD=$3
#SCHEMA="sandbox"
SCHEMA=$4
#SCRIPT="${HOME}/groupon-seo-admin/script"
SCRIPT=$5
#TABLE_NAME="seo_categories_with_names"
TABLE_NAME=$6
#HIERARCHY_TABLE_NAME="seo_category_hierarchy"
HIERARCHY_TABLE_NAME=$7
NUM_TO_KEEP=2
#DUMP_DIR="${HOME}/dump/categories"
DUMP_DIR=$8

# make sure we can find tdsql and tdload and needed libs/modules
#. ~/.bashrc
export PATH=${PATH}:/usr/local/bin/:/usr/local/lib/teradata/client/14.00/tbuild/bin
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib/teradata/client/14.00/tbuild/lib
export TWB_ROOT=/usr/local/lib/teradata/client/14.00/tbuild

echo "Parsing categories"
#python ${SCRIPT}/ddl/parse_categories.py || exit 1

echo "Dropping Teradata checkpoint file if it exists"
#twbrmcp juicer

echo "Dropping table ${SCHEMA}.${TABLE_NAME} and any error tables which might exist"
# ok if these fail, so don't exit on error
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "DROP TABLE ${SCHEMA}.${TABLE_NAME}_Log"
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "DROP TABLE ${SCHEMA}.${TABLE_NAME}_UV"
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "DROP TABLE ${SCHEMA}.${TABLE_NAME}_ET"
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "DROP TABLE ${SCHEMA}.${TABLE_NAME}"

echo "Creating table ${SCHEMA}.${TABLE_NAME}" 
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} < ${SCRIPT}/ddl/${TABLE_NAME}.ddl || exit 1

echo "Loading data to ${SCHEMA}.${TABLE_NAME}"
#tdload -h ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} -f ${DUMP_DIR}/current/categories_parsed.tsv -d "TAB" -t ${TABLE_NAME} --TargetWorkingDatabase ${SCHEMA} || exit 1 

#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "insert into ${SCHEMA}.${TABLE_NAME} select * from sandbox.seo_old_to_new_categories"

echo "Grant public select access to table"
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "GRANT SELECT ON ${SCHEMA}.${TABLE_NAME} TO PUBLIC"

echo "Creating hierarchy table"
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "DROP TABLE ${SCHEMA}.${HIERARCHY_TABLE_NAME}"
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} < ${SCRIPT}/ddl/${HIERARCHY_TABLE_NAME}.ddl || exit 

echo "Grant public select access to table"
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "GRANT SELECT ON ${SCHEMA}.${HIERARCHY_TABLE_NAME} TO PUBLIC"
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "GRANT SELECT ON ${SCHEMA}.${TABLE_NAME} TO PUBLIC"

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

#echo "Copying to MySQL"
#the following are going to become seperate jobs
#${SCRIPT}/copy_to_mysql ${TABLE_NAME}
#${SCRIPT}/copy_to_mysql ${HIERARCHY_TABLE_NAME}

echo "Done"
