tdsql -H $1 -u $2 -p $3 "select $4 from $5.$6 group by 1"
