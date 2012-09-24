register 'akela-0.4-SNAPSHOT.jar'
register 'fhr-toolbox-0.1-SNAPSHOT.jar'
register 'datafu-0.0.4.jar'

SET pig.logfile fhr_crash_analysis.log;
/* SET default_parallel 8; */
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

/* %declare TIME_FORMAT 'yyyy-MM-dd' */
define IsMap com.mozilla.pig.filter.map.IsMap();
define DateDelta com.mozilla.pig.eval.date.TimeDelta('5','yyyy-MM-dd');
define WeekInYear com.mozilla.pig.eval.date.ConvertDateFormat('yyyy-MM-dd', 'w');
define MonthInYear com.mozilla.pig.eval.date.ConvertDateFormat('yyyy-MM-dd', 'M');
define OsVersionNormalizer com.mozilla.pig.eval.regex.FindOrReturn('^[0-9]+(\\.*[0-9]*){1}');
define BucketAddonCount com.mozilla.pig.eval.Bucket('1','2','3','4','5','6');
define Quantile datafu.pig.stats.StreamingQuantile('0.0','0.25','0.5','0.75','1.0');
define CrashTuples com.mozilla.fhr.pig.eval.CrashTuples();

raw = LOAD 'hbase://metrics' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS (k:chararray,json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];

/* Daily */
/* 
Probably going to be best to create a new UDF for crash tuples (date, crash_count, extensions, plugins, etc.) 
because we need to do different looks by day, week of year, month of year, d-7, and d-30
*/
prefltrd = FILTER genmap BY IsMap(json_map#'dataPoints');
data = FOREACH prefltrd GENERATE k,
                                 json_map#'appName' AS product:chararray,
                                 json_map#'appVersion' AS product_version:chararray,
                                 json_map#'appUpdateChannel' AS product_channel:chararray,
                                 json_map#'OSName' AS os:chararray,
                                 OsVersionNormalizer((chararray)json_map#'OSVersion') AS os_version:chararray,
                                 json_map#'locale' AS locale:chararray,
                                 FLATTEN(CrashTuples(json_map#'dataPoints')) AS 
                                    (date:chararray, crash_count_pending:int, crash_count_submitted:int,
                                     aborted_sessions:int, aborted_time:int, aborted_active_time:int,
                                     theme_count:int, ext_count:int, plugin_count:int);
fltrd = FILTER data BY crash_count_pending > 0 OR crash_count_submitted > 0;
bucketed_data = FOREACH fltrd GENERATE k, product,product_version,product_channel,os,os_version,locale,date,
                                      DateDelta(date, '$date') AS days_ago:long,
                                      WeekInYear(date) AS week_in_year:chararray,
                                      MonthInYear(date) AS month_in_year:chararray,
                                      crash_count_pending, crash_count_submitted, 
                                      (crash_count_pending + crash_count_submitted) AS crash_count:int,
                                      aborted_sessions, aborted_time, aborted_active_time,
                                      theme_count, BucketAddonCount(ext_count) AS addon_count:int, plugin_count;


/* Daily */
daily_data = FILTER bucketed_data BY date == '$date';
grouped_daily = GROUP daily_data BY (product,product_version,product_channel,os,os_version,locale,addon_count);
daily_counts = FOREACH grouped_daily GENERATE FLATTEN(group) AS (product,product_version,product_channel,os,os_version,locale,addon_count), 
                                              Quantile(daily_data.crash_count),
                                              SUM(daily_data.crash_count) AS sum_crash_count:long,
                                              COUNT(daily_data) AS doc_count:long;
STORE daily_counts INTO 'fhr-daily-crash-analysis';

/* Week in Year */
wiy_data = FILTER bucketed_data BY week_in_year == '$week';
grouped_wiy = GROUP wiy_data BY (product,product_version,product_channel,os,os_version,locale,addon_count);
wiy_counts = FOREACH grouped_wiy {
    dist_keys = DISTINCT wiy_data.k;
    GENERATE 
        FLATTEN(group) AS (product,product_version,product_channel,os,os_version,locale,addon_count),
        Quantile(wiy_data.crash_count),
        SUM(wiy_data.crash_count) AS sum_crash_count:long,
        COUNT(dist_keys) AS doc_count;
}
STORE wiy_counts INTO 'fhr-wiy-crash-analysis';

/* Month in Year */
miy_data = FILTER bucketed_data BY month_in_year == '$month';
grouped_miy = GROUP miy_data BY (product,product_version,product_channel,os,os_version,locale,addon_count);
miy_counts = FOREACH grouped_miy {
    dist_keys = DISTINCT miy_data.k;
    GENERATE 
        FLATTEN(group) AS (product,product_version,product_channel,os,os_version,locale,addon_count),
        Quantile(miy_data.crash_count),
        SUM(miy_data.crash_count) AS sum_crash_count:long,
        COUNT(dist_keys) AS doc_count;
}
STORE miy_counts INTO 'fhr-miy-crash-analysis';

/* d-7 */
d7_data = FILTER bucketed_data BY days_ago <= 7;
grouped_d7 = GROUP d7_data BY (product,product_version,product_channel,os,os_version,locale,addon_count);
d7_counts = FOREACH grouped_d7 GENERATE FLATTEN(group) AS (product,product_version,product_channel,os,os_version,locale,addon_count), 
                                        Quantile(d7_data.crash_count),
                                        SUM(d7_data.crash_count) AS sum_crash_count:long,
                                        COUNT(d7_data) AS doc_count:long;
STORE d7_counts INTO 'fhr-d7-crash-analysis';

/* d-30 */
d30_data = FILTER bucketed_data BY days_ago <= 30;
grouped_d30 = GROUP d30_data BY (product,product_version,product_channel,os,os_version,locale,addon_count);
d30_counts = FOREACH grouped_d30 GENERATE FLATTEN(group) AS (product,product_version,product_channel,os,os_version,locale,addon_count), 
                                          Quantile(d30_data.crash_count),
                                          SUM(d30_data.crash_count) AS sum_crash_count:long,
                                          COUNT(d30_data) AS doc_count:long;
STORE d30_counts INTO 'fhr-d30-crash-analysis';
