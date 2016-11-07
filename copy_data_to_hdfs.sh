#---------------------------------------------------------------------------------------------------------------------------
# This script calls Calling util.copy_data_to_hdfs() to push data from local file system to HDFS along with partitions
# This script takes 3 arguments
# 1. LOCAL_PATH :  path of the file or Dir which need to be pushed to HDFS i.g "/tmp/abc/a.csv or /tmp/abc/"
# 2. HDFS_PATH : HDFS directiry path where data needs to be copied eg. "hdfs://namdenode/tmp/abc"
# 3. LIST OF PARTITIONS: list of partitions seprated by , e.g "YEAR=2016,MONTH=12"
#                       This is not case sensitive
#                       values should be seprated by "," and should not contain any space in between
#----------------------------------------------------------------------------------------------------------------------------

#Import section
. $BDP_HOME/util/util.sh

#Some Variables for current date
YEAR=`date +'%Y'`
MONTH=`date +'%m' | sed 's/0//g'`
DAY=`date +'%d' | sed 's/0//g'`


#checking no of arguments, this script accepts only 3 arguments
if [[ $# -eq 3 ]]; then
  LOCAL_PATH="$1"
  HDFS_PATH="$2"
  PARTITIONS_STR=`echo $3 | sed 's/,/ /g'`

  copy_data_to_hdfs "$LOCAL_PATH" "$HDFS_PATH" "$PARTITIONS_STR"

else
  echo "ERROR ! Please check no of arguments."
  echo "USAGE : ./copy_data_to_hdfs.sh [LOCAL_PATH] [HDFS_PATH] [0 | year=12,month=10]"
  exit 1
fi

exit 0
