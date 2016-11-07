use ${hiveconf:DB};

DROP TABLE wrk_tmp_ingestion_meradata;
CREATE TABLE wrk_tmp_ingestion_meradata AS
select split("${hiveconf:VALUES}","\\|") as VALUES;

INSERT INTO TABLE ${hiveconf:$TABLE}
select
if(lower(values[0])='null',NULL,values[0]),
if(lower(values[1])='null',NULL,values[1]),
if(lower(values[2])='null',NULL,values[2]),
if(lower(values[3])='null',NULL,values[3]),
if(lower(values[4])='null',NULL,values[4]),
if(lower(values[5])='null',NULL,values[5]),
if(lower(values[6])='null',NULL,values[6]),
if(lower(values[7])='null',NULL,values[7]),
if(lower(values[8])='null',NULL,values[8]),
if(lower(values[9])='null',NULL,values[9]),
if(lower(values[10])='null',NULL,values[10]),
if(lower(values[11])='null',NULL,values[11]),
if(lower(values[12])='null',NULL,values[12]),
split(values[13],","),
split(values[14],","),
if(lower(values[15])='null',NULL,values[15]),
if(lower(values[16])='null',NULL,values[16]),
if(lower(values[17])='null',NULL,values[17]),
if(lower(values[18])='null',NULL,values[18])
from wrk_tmp_ingestion_meradata;
