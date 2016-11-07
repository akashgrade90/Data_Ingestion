#!/bin/bash
#-----------------------------------------------------------------------------------------------------------------------------
#This file provides some common ustilities like copying data to HDFS, Mailing capablities about each job, Ingestion of load metrics
#-----------------------------------------------------------------------------------------------------------------------------


#import section
. $BDP_HOME/properties/dev.properties
#Some Variables for current date
YEAR=`date +'%Y'`
MONTH=`date +'%m' | sed 's/0//g'`
DAY=`date +'%d' | sed 's/0//g'`


#---------------------copy_data_to_hdfs-----------------------------------------
#This function copy the data from local(Ingestion/edge node) to the HDFS cluster
#This function accepts 3 arguments
#1. LOCAL_PATH
#2. HDFS_PATH
#3. Array of PARTITIONS along with its values which needs to be created at HDFS_PATH before putting the data
#    This argument can take two values : 0 i.e user doesn't want to create any partition
#                                     any partition array like:  ('YEAR=2016' 'MONTH=2' 'DAY=21')
#                                                             or ('YEAR=2016' 'MONTH=2' 'DAY=21' 'LOAD_ID=1234')
#-------------------------------------------------------------------------------
copy_data_to_hdfs()
{
 LOCAL_PATH="$1"
 HDFS_PATH="$2"
 PARTITIONS_ARRAY=($3)
 PARTITIONS_ARRAY_PATH=""


 # IF this condition is false i.e not equal to "0"
 #it means user does not want to create a partitions or he/she just want to push to the HDFS_PATH
 if [[ "$PARTITIONS_ARRAY" !=  "0" ]]; then
  #creating a string like /YEAR=2016/MONTH=2/DAY=12
   for (( i=0; i<${#PARTITIONS_ARRAY[@]}; i++ ))
   do
    TEMP=${PARTITIONS_ARRAY[i]}
    PARTITIONS_ARRAY_PATH=$PARTITIONS_ARRAY_PATH\/$TEMP
   done
 fi

 #This the HDFS path
 HDFS_PATH=$GLOBAL_HDFS_PATH$PARTITIONS_ARRAY_PATH

 #Checking if the partition requested by user already exist HDFS_CHECK=0 if dir exist without a file and N if it has N files
 HDFS_CHECK=`hadoop fs -ls  $HDFS_PATH | wc -l`
 if [[ $HDFS_CHECK -eq 0 ]]; then
  #if it is empty or does not exist
  hadoop fs -rmr $HDFS_PATH
  hadoop fs -mkdir $HDFS_PATH
 fi

 #Pushing data from Local path to HDFS path
 hadoop fs -CopyFromLocal $LOCAL_PATH $HDFS_PATH

 #Checking status of CopyFromLocal
  if [[ $? -eq 0 ]]; then
    echo "SUCCESSFUL : data pushed to HDFS"
  else
    echo "FAILED : record could not be push data to HDFS. pls check logs"
    exit 1
  fi

  return 0
}


#----------------------------mail_status----------------------------------------
#This function sends an email to the user once a process/job is finished or failed
#This function also INSERT status of each process in the hive table
#This accepts two arguments
#1. PROCESS_NAME e.g. "COPS_VODAFONE_IT_LOAD"
#2. PROCESS_STATUS e.g. 0 or non-zero
#This function also access one mailing_list and METADATA_DB_NAME which is declared under [env].properties
#-------------------------------------------------------------------------------

mail_status()
{
  PROCESS_NAME=$1
  STATUS=$2

  echo $MAILING_LIST
  echo $PROCESS_NAME

  if [ $STATUS -eq 0 ]; then
    SUBJECT_CONTENT="$PROCESS_NAME has completed!"
  else
    SUBJECT_CONTENT="$PROCESS_NAME has failed!"
  fi
  MESSAGE_CONTENT='hi'

  echo $MESSAGE_CONTENT |  mail -s $SUBJECT_CONTENT  $MAILING_LIST

  #inserting record in database with current date as partition

  return 0
}


#----------------------------hive_ingestion_metadata_insert----------------------------------------
#This function inserts a row into table METADATA_TABLE_NAME which has metadata for the data Ingestion. this variable is specified in the [env].properties
#This accepts only 1 argument
#1. VALUES  e.g. "vf_it|dev|cops|null|raw_it|file|null|path|null|path|mytable|raw_it|vf_ingestion_metric|filemerge,ingest|year,month|data|null|null|1"
#No. of values should be equal to no of columns in the table. the value can also be checked from [env].properties METADATA_TABLE_NO_OF_COL
#Also string should be seprated by the sepration specified in the [env].properties  METADATA_TABLE_FIELD_SEPRATOR e.g. |
#-------------------------------------------------------------------------------

hive_ingestion_metadata_insert()
{
  VALUES=$1
  NO_OF_FIELDS=`echo $VALUES | awk -F"$METADATA_TABLE_FIELD_SEPRATOR" '{print NF}'`

  #if the no of values in the string does not match with the column count of metadata table
  if [[ $NO_OF_FIELDS -ne  $METADATA_TABLE_NO_OF_COL ]]; then
    echo "ERROR : Metadata table $METADATA_TABLE_NAME has $METADATA_TABLE_NO_OF_COL columns but input string has $NO_OF_FIELDS"
    exit 1
  fi

  #Calling hive script to insert the records in the table
  hive -f insert_into_metadata_table.hql --hiveconf DB=$METADATA_DB_NAME --hiveconf TABLE=$METADATA_TABLE_NAME -hiveconf VALUES=$VALUES

  #Checking status of hive query
  if [[ $? -eq 0 ]]; then
    echo "SUCCESSFUL : record inserted in metadata table"
  else
    echo "FAILED : record could not be inserted in metadata table. pls check logs"
    exit 1
  fi
  return 0
}


#----------------------------hive_ingestion_metadata_delete----------------------------------------
#This function deletes a row into table METADATA_TABLE_NAME which has metadata for the data Ingestion. this variable is specified in the [env].properties
#This accepts only 1 argument
# Array of column values(KEYS) which needs to be deleted e.g ("GROUP_ID=vf_it" "ENV_DESC=dev" "SOURCE_NAME=cops" "TARGET_TBL=topup")
# You can chose any combination of values eg you can also chose ("SOURCE_NAME=cops" "TARGET_TBL=topup") this will deleted topup from all the opcos
#The above arguments will be passed if you don't wana load cops.topup for all the markets
#-------------------------------------------------------------------------------

hive_ingestion_metadata_delete()
{
  KEYS=($1)
  QUERY="delete from $METADATA_DB_NAME.$METADATA_TABLE_NAME"
  WHERE_CONDITION=" where "

  #checking if the inpurt array does not have any value
  if [[ ${#KEYS[@]} -eq 0 ]]; then
     echo "ERROR : Empty array no condition provided!"
     exit 1
  fi


  #Building the WHERE condition based on earch value in array
  for (( i=0; i<${#KEYS[@]}; i++ ))
  do
     #Converting SOURCE_NAME=cops => SOURCE_NAME='cops'
     TEMP=`echo ${KEYS[i]} | sed -e "s/=/='/g" -e "s/$/'/g"`
     if [[ $i -eq 0 ]]; then
       AND=""
     else
       AND=" and "
     fi

     WHERE_CONDITION="$WHERE_CONDITION$AND$TEMP"
  done

  QUERY=$QUERY$WHERE_CONDITION

  hive -e "$QUERY"
  #Checking status of hive query
  if [[ $? -eq 0 ]]; then
    echo "SUCCESSFUL : record inserted in metadata table"
  else
    echo "FAILED : record could not be inserted in metadata table. pls check logs"
    exit 1
  fi

  return 0
}



#----------------------------uncompress----------------------------------------
#This function uncompress all the files present in the local directiry of ingestion node
#This accepts only 1 argument : filedirectory path  or file e.g. /vf_it/cops/ or /vf/it/cops/test.tar.gz
#-------------------------------------------------------------------------------
uncompress()
{
  FILEPATH=($1)

  #un-tar a single file and move in same dir
   un_tar()
   {
      #Work on this some file might have other types as well like zip or only tar
      mv `tar zxvf $1` $2 > /dev/null 2>&1
   }


  if [[ "$FILEPATH" =~ ".tar" ]]; then
     echo "$FILEPATH is a file. uncompressing this file"
     DIR=`dirname "$FILEPATH"`
     un_tar $FILEPATH $DIR

  elif [[ -f $FILEPATH ]]; then
    echo "$FILEPATH is not a tar file nor dir"
  else
    echo "$FILEPATH is a directiry. uncompressing all the files under this folder"
      for files in $FILEPATH/*
      do
        un_tar "$files" $FILEPATH
      done
  fi

  return 0
}
