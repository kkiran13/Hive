$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS stock"

$HIVE_HOME/bin/hive -e "CREATE TABLE stock (
  DAT string,OPEN string,HIGH string,LOW string,CLOSE string,VOLUME string,ADJCLOSE string
) row format delimited fields terminated by ',' stored as textfile;"

$HIVE_HOME/bin/hive -e "LOAD DATA LOCAL INPATH '$1/*.csv' OVERWRITE INTO TABLE stock;"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS stockin"
$HIVE_HOME/bin/hive -e "CREATE TABLE stockin row format delimited fields terminated by '\t' stored as textfile as select INPUT__FILE__NAME a,DAT b,ADJCLOSE c from stock;"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS stock"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS stockreqfd"
$HIVE_HOME/bin/hive -e "CREATE TABLE stockreqfd row format delimited fields terminated by '\t' stored as textfile as select concat(a,'+',substr(b,0,7)) kkey, concat(substr(b,9,10),c) vval from stockin SORT BY vval ASC;"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS stockin"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS mindata"
$HIVE_HOME/bin/hive -e "CREATE TABLE mindata row format delimited fields terminated by '\t' stored as textfile as select kkey,vval from 
(select kkey,vval,row_number() over (partition by kkey order by vval ASC) r from stockreqfd) z 
where r = 1;"	

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS maxdata"
$HIVE_HOME/bin/hive -e "CREATE TABLE maxdata row format delimited fields terminated by '\t' stored as textfile as select kkey,vval from 
(select kkey,vval,row_number() over (partition by kkey order by vval DESC) r from stockreqfd) z 
where r = 1;"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS stockreqfd"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS joindata"
$HIVE_HOME/bin/hive -e "CREATE TABLE joindata row format delimited fields terminated by '\t' stored as textfile as select substr(mi.kkey,1,length(mi.kkey)-8) kkey,substr(ma.vval,3,length(ma.vval)) maval,substr(mi.vval,3,length(mi.vval)) mival, ((substr(ma.vval,3,length(ma.vval))-substr(mi.vval,3,length(mi.vval)))/substr(mi.vval,3,length(mi.vval))) xi from mindata mi,maxdata ma where mi.kkey=ma.kkey;"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS mindata"
$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS maxdata"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS xbardata"
$HIVE_HOME/bin/hive -e "CREATE TABLE xbardata row format delimited fields terminated by '\t' stored as textfile as select kkey,AVG(xi) xbar from joindata group by kkey;"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS fdata"
$HIVE_HOME/bin/hive -e "CREATE TABLE fdata row format delimited fields terminated by '\t' stored as textfile as select jd.kkey,jd.xi-xbd.xbar,(jd.xi-xbd.xbar)*(jd.xi-xbd.xbar) sq from joindata jd,xbardata xbd where jd.kkey=xbd.kkey;"
#$HIVE_HOME/bin/hive -e "select * from fdata;"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS joindata"
$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS xbardata"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS volat_t"
$HIVE_HOME/bin/hive -e "CREATE TABLE volat_t row format delimited fields terminated by '\t' stored as textfile as select kkey stock,SQRT(SUM(sq)/(count(kkey)-1)) volatility from fdata group by kkey;"

echo "******Minimum Ten Stocks************"
$HIVE_HOME/bin/hive -e "select * from volat_t where volatility is not NULL and volatility!=0 order by volatility asc limit 10;"

echo "******Maximum Ten Stocks************"
$HIVE_HOME/bin/hive -e "select * from volat_t order by volatility desc limit 10;"