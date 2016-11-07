#-----------------------------------------------------------------------------------------------------------------------------------
# This script calls Calling util.hive_ingestion_metadata_insert() for each line the argument file
# argument file must contain data in below format and it can contain n no of rows which needs to be inserted in the metadata table
# vf_it||null|null|null|file|null|path|null|path|mytable|raw_it|vf_ingestion_metric|filemerge,ingest|year,month|data|null|null|1
# Note LIST OF TASKS (14th column) column and LIST OF PARTITIONS (15th column) will be seprated by ","
#-----------------------------------------------------------------------------------------------------------------------------------

#import section
. $BDP_HOME/util/util.sh
#Some Variables for current date
YEAR=`date +'%Y'`
MONTH=`date +'%m' | sed 's/0//g'`
DAY=`date +'%d' | sed 's/0//g'`


#checking no of arguments, this script accepts only one argument and it should be a file with full path
if [[ $# -eq 1 ]]; then

  #checking if argument is a file
  if [[ -f $1 ]]; then
    echo "Calling util.hive_ingestion_metadata_insert() for each value in file $1"

    while read line
    do
      hive_ingestion_metadata_insert $line
    done <$1

  else
    echo "ERROR ! Please check the arguments. Argument is not a file"
    echo "USAGE : ./hive_ingestion_metadata_insert.sh [path of the file containing values]"
    exit 1
  fi
else
  echo "ERROR ! Please check no of arguments."
  echo "USAGE : ./hive_ingestion_metadata_insert.sh [path of the file containing values]"
  exit 1
fi

exit 0
