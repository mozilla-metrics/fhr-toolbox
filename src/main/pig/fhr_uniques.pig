register 'akela-0.5-SNAPSHOT.jar'
register 'fhr-toolbox-0.1-SNAPSHOT.jar'
register 'datafu-0.0.4.jar'
register 'vertica-jdk5-6.0.0-0.jar'
register 'pig-vertica.jar'
register 'jackson-core-2.0.6.jar'
register 'jackson-databind-2.0.6.jar'
register 'jackson-annotations-2.0.6.jar'

SET pig.logfile fhr_uniques.log;
/* SET default_parallel 8; */
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

/* %declare TIME_FORMAT 'yyyy-MM-dd'; */
define DaysAgo com.mozilla.pig.eval.date.TimeDelta('5', 'yyyy-MM-dd');
define DaysAgoTimestamps com.mozilla.pig.eval.date.TimeDelta('5');
define BucketProfileAge com.mozilla.pig.eval.Bucket('1','7','30','180','365','366');
define ParseDate com.mozilla.pig.eval.date.ParseDate('yyyy-MM-dd');
define FormatDate com.mozilla.pig.eval.date.FormatDate('yyyy-MM-dd');
define WeekInYear com.mozilla.pig.eval.date.ConvertDateFormat('yyyy-MM-dd', 'w');
define MonthInYear com.mozilla.pig.eval.date.ConvertDateFormat('yyyy-MM-dd', 'M');
define Year com.mozilla.pig.eval.date.ConvertDateFormat('yyyy-MM-dd', 'yyyy');
define Median datafu.pig.stats.Median();
define OsVersionNormalizer com.mozilla.pig.eval.regex.FindOrReturn('^[0-9]+(\\.*[0-9]*){1}');
define VersionOnDate com.mozilla.fhr.pig.eval.VersionOnDate('yyyy-MM-dd', '$date');
define FirstPingTime com.mozilla.fhr.pig.eval.FirstPingTime('yyyy-MM-dd');
define LatestPingTime com.mozilla.fhr.pig.eval.LatestPingTime('yyyy-MM-dd', '$date');
define ProfileCreationTime com.mozilla.fhr.pig.eval.ProfileCreationTime();
define IsMap com.mozilla.pig.filter.map.IsMap();

raw = LOAD 'hbase://metrics' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS 
                                   (k:bytearray,json:chararray);
genmap = FOREACH raw GENERATE com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY IsMap(json_map#'data'#'days');
data = FOREACH filtered_genmap GENERATE LatestPingTime(json_map#'data'#'days') AS latest_time:long,
                               FirstPingTime(json_map#'data'#'days') AS first_time:long,
                               json_map#'data'#'last'#'org.mozilla.appInfo.appinfo'#'name' AS product:chararray,
                               VersionOnDate(json_map#'data'#'days') AS product_version:chararray,
                               json_map#'data'#'last'#'org.mozilla.appInfo.appinfo'#'updateChannel' AS product_channel:chararray,
                               json_map#'data'#'last'#'org.mozilla.appInfo.appinfo'#'os' AS os:chararray,
                               OsVersionNormalizer((chararray)json_map#'data'#'last'#'org.mozilla.sysinfo.sysinfo'#'version') AS os_version:chararray,
                               json_map#'geoCountry' AS geo_country_code:chararray,
                               (int)DaysAgoTimestamps(ProfileCreationTime((int)json_map#'data'#'last'#'org.mozilla.profile.age'#'profileCreation'), ParseDate('$date')) AS profile_age:int;
filtered_data = FILTER data BY latest_time IS NOT NULL AND 
                               profile_age IS NOT NULL AND profile_age >= 0 AND
                               product IS NOT NULL AND 
                               (product == 'Firefox' OR product == 'MetroFirefox' OR 
                                product == 'Fennec' OR product == 'Thunderbird') AND
                               product_version IS NOT NULL AND
                               product_channel IS NOT NULL AND
                               os IS NOT NULL AND
                               os_version IS NOT NULL;
converted_ping_time = FOREACH filtered_data GENERATE *, FormatDate(latest_time) AS ping_time:chararray;
adjusted_data = FOREACH converted_ping_time GENERATE ping_time,
                                                     DaysAgo(ping_time, '$date') AS days_ago:long, /* calc days ago from specified day */
                                                     (long)WeekInYear(ping_time) AS week_in_year:long,
                                                     (long)MonthInYear(ping_time) AS month_in_year:long,
                                                     (long)Year(ping_time) AS year:long,
                                                     product, product_version, product_channel,
                                                     os, os_version, geo_country_code,
                                                     (latest_time == first_time ? 1 : 0) AS new_ping:int,
                                                     profile_age,
                                                     BucketProfileAge(profile_age) AS bucketed_profile_age:int;
/* clean data for DB insertion */
cleansed_data = FILTER adjusted_data BY SIZE(product) <= 32 AND
                                        SIZE(product_version) <= 8 AND
                                        SIZE(product_channel) <= 64 AND
                                        SIZE(os) <= 16 AND
                                        SIZE(os_version) <= 32 AND
                                        SIZE(geo_country_code) <= 4 AND
                                        new_ping IS NOT NULL AND
                                        profile_age IS NOT NULL AND
                                        bucketed_profile_age IS NOT NULL;

/* Daily */
daily_data = FILTER cleansed_data BY days_ago == 0;
grouped_daily = GROUP daily_data BY (product,product_version,product_channel,os,os_version,geo_country_code,new_ping);
daily_counts = FOREACH grouped_daily GENERATE '$date' AS perspective_date:chararray,
                                              FLATTEN(group) AS (product,product_version,product_channel,os,os_version,geo_country_code,new_ping), 
                                              FLATTEN(Median(daily_data.bucketed_profile_age)) AS median_profile_age:float,
                                              COUNT(daily_data) AS count:long;
/*STORE daily_counts INTO 'fhr-daily-counts-$date';*/
/* Store into Vertica (only will work on Vertica 5+ and the vertica connector jar needs to be on every machine)*/
STORE daily_counts INTO '{fhr_daily_counts(perspective_date date, product varchar(32), product_version varchar(8), product_channel varchar(64), os varchar(16), os_version varchar(32), geo_country_code varchar(4), new_ping int, median_profile_age float, count int)}' USING com.vertica.pig.VerticaStorer('$dblist', '$dbname', '$dbport', '$dbuser', '$dbpass');



/* Week in Year */
weekly_data = FILTER cleansed_data BY week_in_year == $week AND year == $year;
grouped_wiy = GROUP weekly_data BY (product,product_version,product_channel,os,os_version,geo_country_code,new_ping);
wiy_counts = FOREACH grouped_wiy GENERATE '$date' AS perspective_date:chararray,
                                          FLATTEN(group) AS (product,product_version,product_channel,os,os_version,geo_country_code,new_ping), 
                                          FLATTEN(Median(weekly_data.bucketed_profile_age)) AS median_profile_age,
                                          COUNT(weekly_data) AS count:long;
/*STORE wiy_counts INTO 'fhr-wiy-counts-$date';*/
/* Store into Vertica (only will work on Vertica 5+ and the vertica connector jar needs to be on every machine)*/
STORE wiy_counts INTO '{fhr_wiy_counts(perspective_date date, product varchar(32), product_version varchar(8), product_channel varchar(64), os varchar(16), os_version varchar(32), geo_country_code varchar(4), new_ping int, median_profile_age float, count int)}' USING com.vertica.pig.VerticaStorer('$dblist', '$dbname', '$dbport', '$dbuser', '$dbpass');
                   
/* Month in Year */
monthly_data = FILTER cleansed_data BY month_in_year == $month AND year == $year;
grouped_miy = GROUP monthly_data BY (product,product_version,product_channel,os,os_version,geo_country_code,new_ping);
miy_counts = FOREACH grouped_miy GENERATE '$date' AS perspective_date:chararray,
                                          FLATTEN(group) AS (product,product_version,product_channel,os,os_version,geo_country_code,new_ping), 
                                          FLATTEN(Median(monthly_data.bucketed_profile_age)) AS median_profile_age,
                                          COUNT(monthly_data) AS count:long;
/*STORE miy_counts INTO 'fhr-miy-counts-$date';*/
/* Store into Vertica (only will work on Vertica 5+ and the vertica connector jar needs to be on every machine)*/
STORE miy_counts INTO '{fhr_miy_counts(perspective_date date, product varchar(32), product_version varchar(8), product_channel varchar(64), os varchar(16), os_version varchar(32), geo_country_code varchar(4), new_ping int, median_profile_age float, count int)}' USING com.vertica.pig.VerticaStorer('$dblist', '$dbname', '$dbport', '$dbuser', '$dbpass');
                   
/* d-7 to d */
d7_data = FILTER cleansed_data BY days_ago >= 0 AND days_ago < 7;
grouped_d7 = GROUP d7_data BY (product,product_version,product_channel,os,os_version,geo_country_code,new_ping);
d7_counts = FOREACH grouped_d7 GENERATE '$date' AS perspective_date:chararray,
                                        FLATTEN(group) AS (product,product_version,product_channel,os,os_version,geo_country_code,new_ping),  
                                        FLATTEN(Median(d7_data.bucketed_profile_age)) AS median_profile_age,
                                        COUNT(d7_data) AS count:long;
/*STORE d7_counts INTO 'fhr-d7-counts-$date';*/
/* Store into Vertica (only will work on Vertica 5+ and the vertica connector jar needs to be on every machine)*/
STORE d7_counts INTO '{fhr_d7_counts(perspective_date date, product varchar(32), product_version varchar(8), product_channel varchar(64), os varchar(16), os_version varchar(32), geo_country_code varchar(4), new_ping int, median_profile_age float, count int)}' USING com.vertica.pig.VerticaStorer('$dblist', '$dbname', '$dbport', '$dbuser', '$dbpass');
                   
/* d-30 to d */
d30_data = FILTER cleansed_data BY days_ago >= 0 AND days_ago < 30;
grouped_d30 = GROUP d30_data BY (product,product_version,product_channel,os,os_version,geo_country_code,new_ping);
d30_counts = FOREACH grouped_d30 GENERATE '$date' AS perspective_date:chararray,
                                          FLATTEN(group) AS (product,product_version,product_channel,os,os_version,geo_country_code,new_ping), 
                                          FLATTEN(Median(d30_data.bucketed_profile_age)) AS median_profile_age,
                                          COUNT(d30_data) AS count:long;
/*STORE d30_counts INTO 'fhr-d30-counts-$date';*/
/* Store into Vertica (only will work on Vertica 5+ and the vertica connector jar needs to be on every machine)*/
STORE d30_counts INTO '{fhr_d30_counts(perspective_date date, product varchar(32), product_version varchar(8), product_channel varchar(64), os varchar(16), os_version varchar(32), geo_country_code varchar(4), new_ping int, median_profile_age float, count int)}' USING com.vertica.pig.VerticaStorer('$dblist', '$dbname', '$dbport', '$dbuser', '$dbpass');