#!/bin/bash
#-----------------------------------------------------------------------------------------------------------------------------
#This is the main file responsible for data ingestion. data ingestion can be of two part
# 1. SQOOP  -  delta or full
# 2. FILE - Local to HDFS
#This scripts accepts only 1 argument ie. a list of condition
# e.g you want to load all the sources for vf_it, then poss: OPCO=vf_it
# e.g you want to load cops data for vf_it then poss : OPCO=vf_it,DATA_SOURCE=cops
# e.g you want to load topup data from vf_id of cops datasouce : OPCO=vf_it,DATA_SOURCE=cops,TABLE=topup
# e.g or even if you want to load cops.topup for vf_if and vf_uk (ignoting other opcos): OPCO=vf_it,OPCO=vf_uk,DATA_SOURCE=cops,TABLE=topup
#-----------------------------------------------------------------------------------------------------------------------------
