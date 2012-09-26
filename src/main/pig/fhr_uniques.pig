register 'akela-0.4-SNAPSHOT.jar'
register 'fhr-toolbox-0.1-SNAPSHOT.jar'
register 'datafu-0.0.4.jar'

SET pig.logfile fhr_uniques.log;
/* SET default_parallel 8; */
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

/* %declare TIME_FORMAT 'yyyy-MM-dd'; */
define DaysAgo com.mozilla.pig.eval.date.TimeDelta('5', 'yyyy-MM-dd');
define BucketProfileAge com.mozilla.pig.eval.Bucket('1','7','30','180','365','366');
define ParseDate com.mozilla.pig.eval.date.ParseDate('yyyy-MM-dd');
define WeekInYear com.mozilla.pig.eval.date.ConvertDateFormat('yyyy-MM-dd', 'w');
define MonthInYear com.mozilla.pig.eval.date.ConvertDateFormat('yyyy-MM-dd', 'M');
define Year com.mozilla.pig.eval.date.ConvertDateFormat('yyyy-MM-dd', 'yyyy');
define Median datafu.pig.stats.Median();
define OsVersionNormalizer com.mozilla.pig.eval.regex.FindOrReturn('^[0-9]+(\\.*[0-9]*){1}');

/* define Quantile datafu.pig.stats.StreamingQuantile('0.0','0.25','0.5','0.75','1.0'); */

raw = LOAD 'hbase://metrics' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS 
                                   (k:bytearray,json:chararray);
genmap = FOREACH raw GENERATE com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
data = FOREACH genmap GENERATE json_map#'thisPingTime' AS ping_time:chararray,
                               DaysAgo(json_map#'thisPingTime', '$date') AS days_ago:long, /* calc days ago from specified day */
                               (long)WeekInYear(json_map#'thisPingTime') AS week_in_year:long,
                               (long)MonthInYear(json_map#'thisPingTime') AS month_in_year:long,
                               (long)Year(json_map#'thisPingTime') AS year:long,
                               json_map#'appName' AS product:chararray,
                               json_map#'appVersion' AS product_version:chararray,
                               json_map#'appUpdateChannel' AS product_channel:chararray,
                               json_map#'OSName' AS os:chararray,
                               OsVersionNormalizer((chararray)json_map#'OSVersion') AS os_version:chararray,
                               json_map#'locale' AS locale:chararray,
                               (ParseDate(json_map#'lastPingTime') == 0 ? 1 : 0) AS new_ping:int,
                               json_map#'appProfileAge' AS profile_age:int,
                               BucketProfileAge(json_map#'appProfileAge') AS bucketed_profile_age:int;
 
/* Daily */
daily_data = FILTER data BY days_ago == 1 AND profile_age IS NOT NULL;
/* dump daily_data; */
grouped_daily = GROUP daily_data BY (product,product_version,product_channel,os,os_version,locale,new_ping);
daily_counts = FOREACH grouped_daily GENERATE FLATTEN(group) AS (product,product_version,product_channel,os,os_version,locale,new_ping), 
                                              FLATTEN(Median(daily_data.profile_age)) AS median_profile_age, 
                                              FLATTEN(Median(daily_data.bucketed_profile_age)) AS median_bucketed_profile_age, 
                                              COUNT(daily_data) AS count:long;
STORE daily_counts INTO 'fhr-daily-counts';
/* Store into Vertica (only will work on Vertica 5+ and the vertica connector jar needs to be on every machine)*/
/* STORE daily_counts INTO '{fhr_daily_counts(ping_time date, product varchar(32), product_version varchar(8), product_channel varchar(16), os varchar(16), os_version varchar(32), locale varchar(8), new_ping int, profile_age float, count int)}' USING com.vertica.pig.VerticaStorer('localhost', 'metrics', '5433', 'user', 'pass'); */

/* Week in Year */
weekly_data = FILTER data BY week_in_year == $week AND year == $year;
grouped_wiy = GROUP weekly_data BY (product,product_version,product_channel,os,os_version,locale,new_ping);
wiy_counts = FOREACH grouped_wiy GENERATE FLATTEN(group) AS (product,product_version,product_channel,os,os_version,locale,new_ping), 
                                          FLATTEN(Median(weekly_data.profile_age)) AS median_profile_age, 
                                          FLATTEN(Median(weekly_data.bucketed_profile_age)) AS median_bucketed_profile_age, 
                                          COUNT(weekly_data) AS count:long;
STORE wiy_counts INTO 'fhr-wiy-counts';

/* Month in Year */
monthly_data = FILTER data BY month_in_year == $month AND year == $year;
grouped_miy = GROUP monthly_data BY (product,product_version,product_channel,os,os_version,locale,new_ping);
miy_counts = FOREACH grouped_miy GENERATE FLATTEN(group) AS (product,product_version,product_channel,os,os_version,locale,new_ping), 
                                          FLATTEN(Median(monthly_data.profile_age)) AS median_profile_age, 
                                          FLATTEN(Median(monthly_data.bucketed_profile_age)) AS median_bucketed_profile_age, 
                                          COUNT(monthly_data) AS count:long;
STORE miy_counts INTO 'fhr-miy-counts';

/* d-7 to d */
d7_data = FILTER data BY days_ago <= 7 AND profile_age IS NOT NULL;
grouped_d7 = GROUP d7_data BY (product,product_version,product_channel,os,os_version,locale,new_ping);
d7_counts = FOREACH grouped_d7 GENERATE FLATTEN(group) AS (product,product_version,product_channel,os,os_version,locale,new_ping), 
                                        FLATTEN(Median(d7_data.profile_age)) AS median_profile_age, 
                                        FLATTEN(Median(d7_data.bucketed_profile_age)) AS median_bucketed_profile_age, 
                                        COUNT(d7_data) AS count:long;
STORE d7_counts INTO 'fhr-d7-counts';

/* d-30 to d */
d30_data = FILTER data BY days_ago <= 30 AND profile_age IS NOT NULL;
grouped_d30 = GROUP d30_data BY (product,product_version,product_channel,os,os_version,locale,new_ping);
d30_counts = FOREACH grouped_d30 GENERATE FLATTEN(group) AS (product,product_version,product_channel,os,os_version,locale,new_ping), 
                                        FLATTEN(Median(d30_data.profile_age)) AS median_profile_age, 
                                        FLATTEN(Median(d30_data.bucketed_profile_age)) AS median_bucketed_profile_age, 
                                        COUNT(d30_data) AS count:long;
STORE d30_counts INTO 'fhr-d30-counts';