register 'akela-0.5-SNAPSHOT.jar'
register 'fhr-toolbox-0.1-SNAPSHOT.jar'
register 'datafu-0.0.4.jar'
register 'vertica-jdk5-6.0.0-0.jar'
register 'pig-vertica.jar'
register 'jackson-core-2.0.6.jar'
register 'jackson-databind-2.0.6.jar'
register 'jackson-annotations-2.0.6.jar'

SET pig.logfile fhr_usage_frequency.log;
/* SET default_parallel 8; */
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

define IsMap com.mozilla.pig.filter.map.IsMap();
define OsVersionNormalizer com.mozilla.pig.eval.regex.FindOrReturn('^[0-9]+(\\.*[0-9]*){1}');
define UsageFrequency com.mozilla.fhr.pig.eval.UsageFrequency('yyyy-MM-dd','$date');
define Median datafu.pig.stats.Median();
define VersionOnDate com.mozilla.fhr.pig.eval.VersionOnDate('yyyy-MM-dd', '$date');

raw = LOAD 'hbase://metrics' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS (k:chararray,json:chararray);
genmap = FOREACH raw GENERATE k, com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
prefltrd = FILTER genmap BY IsMap(json_map#'dataPoints');
data = FOREACH prefltrd GENERATE k,
                               json_map#'appName' AS product:chararray,
                               VersionOnDate(json_map#'versions') AS product_version:chararray,
                               json_map#'appUpdateChannel' AS product_channel:chararray,
                               json_map#'OSName' AS os:chararray,
                               OsVersionNormalizer((chararray)json_map#'OSVersion') AS os_version:chararray,
                               FLATTEN(UsageFrequency(json_map#'dataPoints')) AS usage_interval:long;
postfltrd = FILTER data BY product IS NOT NULL AND 
                           product_version IS NOT NULL AND 
                           product_channel IS NOT NULL AND
                           os IS NOT NULL AND
                           os_version IS NOT NULL AND
                           usage_interval IS NOT NULL;
ordered_data = ORDER postfltrd BY product, product_version, product_channel, os, os_version, usage_interval ASC;
grpd_data = GROUP ordered_data BY (product, product_version, product_channel, os, os_version);
analyzed = FOREACH grpd_data {
    keys = DISTINCT ordered_data.k;
    GENERATE 
        '$date' AS perspective_date:chararray,
        FLATTEN(group) AS (product, product_version, product_channel, os, os_version),
        FLATTEN(Median(ordered_data.usage_interval)) AS median_usage_interval,
        /*Quantile(ordered_data.usage_interval),*/
        COUNT(keys) AS doc_count:long;
}

STORE analyzed INTO 'fhr-usage-frequency-$date';
/* Store into Vertica (only will work on Vertica 5+ and the vertica connector jar needs to be on every machine)*/
STORE analyzed INTO '{fhr_usage_frequency(perspective_date date, product varchar(32), product_version varchar(8), product_channel varchar(16), os varchar(16), os_version varchar(32), median_usage_interval float, doc_count int)}' USING com.vertica.pig.VerticaStorer('$dblist', '$dbname', '$dbport', '$dbuser', '$dbpass');