register 'akela-0.4-SNAPSHOT.jar'
register 'fhr-toolbox-0.1-SNAPSHOT.jar'
register 'datafu-0.0.4.jar'

SET pig.logfile fhr_usage_frequency.log;
/* SET default_parallel 8; */
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

define IsMap com.mozilla.pig.filter.map.IsMap();
define OsVersionNormalizer com.mozilla.pig.eval.regex.FindOrReturn('^[0-9]+(\\.*[0-9]*){1}');
define UsageFrequency com.mozilla.fhr.pig.eval.UsageFrequency();
define Median datafu.pig.stats.StreamingQuantile('0.5');
define Quantile datafu.pig.stats.StreamingQuantile('0.0','0.25','0.5','0.75','1.0');

raw = LOAD 'hbase://metrics' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS (k:chararray,json:chararray);
genmap = FOREACH raw GENERATE k, com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
prefltrd = FILTER genmap BY IsMap(json_map#'dataPoints');
data = FOREACH prefltrd GENERATE k,
                               json_map#'appName' AS product:chararray,
                               json_map#'appVersion' AS product_version:chararray,
                               json_map#'appUpdateChannel' AS product_channel:chararray,
                               json_map#'OSName' AS os:chararray,
                               OsVersionNormalizer((chararray)json_map#'OSVersion') AS os_version:chararray,
                               FLATTEN(UsageFrequency(json_map#'dataPoints')) AS usage_interval:long;
postfltrd = FILTER data BY usage_interval IS NOT NULL;
ordered_data = ORDER postfltrd BY product, product_version, product_channel, os, os_version, usage_interval ASC;
grpd_data = GROUP ordered_data BY (product, product_version, product_channel, os, os_version);
analyzed = FOREACH grpd_data {
    keys = DISTINCT ordered_data.k;
    GENERATE 
        FLATTEN(group) AS (product, product_version, product_channel, os, os_version),
        Median(ordered_data.usage_interval),
        Quantile(ordered_data.usage_interval),
        COUNT(keys);
}

STORE analyzed INTO 'fhr-usage-frequency';