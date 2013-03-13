register 'akela-0.5-SNAPSHOT.jar'
register 'fhr-toolbox-0.1-SNAPSHOT.jar'
register 'jackson-core-2.1.1.jar'
register 'jackson-databind-2.1.1.jar'
register 'jackson-annotations-2.1.1.jar'

SET pig.logfile fhr_payload_errors.log;
/* SET default_parallel 8; */
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

define IsMap com.mozilla.pig.filter.map.IsMap();
define Size com.mozilla.pig.eval.Size();

raw = LOAD 'hbase://metrics' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS 
                                   (k:bytearray,json:chararray);
genmap = FOREACH raw GENERATE k, com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY json_map#'errors' IS NOT NULL;
data = FOREACH filtered_genmap GENERATE json_map#'thisPingDate' AS submission_date:chararray, json_map#'errors' AS errs:chararray;

STORE data INTO 'fhr_payload_errors_out';
