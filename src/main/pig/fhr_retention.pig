register 'akela-0.5-SNAPSHOT.jar'
register 'fhr-toolbox-0.1-SNAPSHOT.jar'
register 'datafu-0.0.4.jar'
register 'vertica-jdk5-6.0.0-0.jar'
register 'pig-vertica.jar'
register 'jackson-core-2.0.6.jar'
register 'jackson-databind-2.0.6.jar'
register 'jackson-annotations-2.0.6.jar'

SET pig.logfile fhr_retention.log;
/* SET default_parallel 8; */
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

/* %declare TIME_FORMAT 'yyyy-MM-dd'; */
define IsMap com.mozilla.pig.filter.map.IsMap();
define ProfileAgeTime com.mozilla.fhr.pig.eval.ProfileAgeTime('yyyy-MM-dd');
define FirstPingTime com.mozilla.fhr.pig.eval.FirstPingTime('yyyy-MM-dd');
define DaysAgo com.mozilla.pig.eval.date.TimeDelta('5', 'yyyy-MM-dd');
define WeekInYear com.mozilla.pig.eval.date.FormatDate('w');
define Year com.mozilla.pig.eval.date.FormatDate('yyyy');
define OsVersionNormalizer com.mozilla.pig.eval.regex.FindOrReturn('^[0-9]+(\\.*[0-9]*){1}');
define WeekDelta com.mozilla.pig.eval.date.TimeDelta('3');
define PingTimes com.mozilla.fhr.pig.eval.PingTimes();
define DistinctByKeyAndWeekDelta datafu.pig.bags.DistinctBy('0','9');
define LatestPingTime com.mozilla.fhr.pig.eval.LatestPingTime('yyyy-MM-dd', '$date');
define FormatDate com.mozilla.pig.eval.date.FormatDate('yyyy-MM-dd');

/* define Quantile datafu.pig.stats.StreamingQuantile('0.0','0.25','0.5','0.75','1.0'); */

/* 
Calculate the probability that a user has used Firefox this week given that their first use was This Week - N 
e.g. - P(User has used Firefox this week | First Use was Week - N) where N = 1,2,3...
*/
raw = LOAD 'hbase://metrics' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS 
                                   (k:bytearray,json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY json_map#'dataPoints' IS NOT NULL AND 
                                   IsMap(json_map#'dataPoints') AND
                                   json_map#'appProfileAge' IS NOT NULL;

grpd_by_all = GROUP filtered_genmap ALL;
n = FOREACH grpd_by_all GENERATE COUNT(filtered_genmap);

data = FOREACH filtered_genmap GENERATE k,
                                        ProfileAgeTime(FormatDate(LatestPingTime(json_map#'dataPoints')), json_map#'appProfileAge') AS profile_age_time:long,
                                        FirstPingTime(json_map#'dataPoints') AS first_ping_time:long, 
                                        PingTimes(json_map#'dataPoints') AS ping_times;
                                        
flat_data = FOREACH data GENERATE k, profile_age_time, FLATTEN(ping_times) AS ping_time:long;
wiy_data = FOREACH flat_data GENERATE k, profile_age_time, ping_time, 
                                      (int)WeekInYear(ping_time) AS week_in_year:int, 
                                      (int)Year(ping_time) AS year:int;
/* Constrain data to before the specified week and year (works in case you ever have to rerun from a historical perspective) */
tc_data = FILTER wiy_data BY week_in_year <= $week AND year <= $year;
key_deltas = FOREACH tc_data GENERATE k, WeekDelta(profile_age_time, ping_time) AS week_delta:long;
distinct_key_deltas = DISTINCT key_deltas;
grpd_by_week_delta = GROUP distinct_key_deltas BY week_delta;
week_delta_counts = FOREACH grpd_by_week_delta GENERATE FLATTEN(group) AS week_delta:long, COUNT(distinct_key_deltas) AS delta_count:long;
week_delta_props = FOREACH week_delta_counts GENERATE week_delta, delta_count, ((double)delta_count/(double)n.$0);
dump week_delta_props;

/* Group by key and only keep the most recent week (i.e. lowest week delta) */
grpd_by_k = GROUP distinct_key_deltas BY k;
min_deltas = FOREACH grpd_by_k GENERATE FLATTEN(group) AS k:bytearray, MIN(distinct_key_deltas.week_delta) AS week_delta:long;
grpd_by_week_delta2 = GROUP min_deltas BY week_delta;
week_delta_counts2 = FOREACH grpd_by_week_delta2 GENERATE FLATTEN(group) AS week_delta:long,
                                                          COUNT(min_deltas) AS delta_count:long;
week_delta_probs = FOREACH week_delta_counts2 GENERATE week_delta, delta_count, ((double)delta_count/(double)n.$0);
dump week_delta_probs;

/* Week in Year */

/* TODO: Calculate set intersection cardinality between current week and week-1, week-2, ... week-N */
/* Probably easiest to do this with a custom function that looks at history date vectors */
/*
STORE wiy_counts INTO 'fhr-wiy-retention';
*/

/* d-7 */
/* TODO: do a d-7 as well if it makes sense after completing the above */