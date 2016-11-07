#-----------------------------------------------------------------------------------------------------------------------------
# This script calls Calling util.hive_ingestion_metadata_delete() for each line the argument file
# This script takes 1 argument
# 1.  argument file must contain data in below format and it can contain n no of rows which needs to be deleted from metadata table
#  GROUP_ID=vf_it,ENV_DESC=dev,SOURCE_NAME=cops,TARGET_TBL=topup
#  This argument is not case sensitive :)
#-----------------------------------------------------------------------------------------------------------------------------


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
    echo "Calling util.hive_ingestion_metadata_delete() for each value in file $1"

    while read line
    do
      xf_line=`echo $line | sed 's/,/ /g'`
      hive_ingestion_metadata_delete $xf_line
    done <$1

  else
    echo "ERROR ! Please check the arguments. Argument is not a file"
    echo "USAGE : ./hive_ingestion_metadata_delete.sh [path of the file containing values]"
    exit 1
  fi
else
  echo "ERROR ! Please check no of arguments."
  echo "USAGE : ./hive_ingestion_metadata_delete.sh [path of the file containing values]"
  exit 1
fi

exit 0
