register 'akela-0.5-SNAPSHOT.jar'
register 'fhr-toolbox-0.1-SNAPSHOT.jar'
register 'datafu-0.0.4.jar'
register 'vertica-jdk5-6.0.0-0.jar'
register 'pig-vertica.jar'
register 'jackson-core-2.0.6.jar'
register 'jackson-databind-2.0.6.jar'
register 'jackson-annotations-2.0.6.jar'

SET pig.logfile fhr_crash_analysis.log;
/* SET default_parallel 8; */
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

define IsMap com.mozilla.pig.filter.map.IsMap();
define SearchTuples com.mozilla.fhr.pig.eval.SearchTuples();

raw = LOAD 'hbase://metrics' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS (k:chararray,json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
prefltrd = FILTER genmap BY IsMap(json_map#'dataPoints');
data = FOREACH prefltrd GENERATE FLATTEN(SearchTuples(json_map#'dataPoints')) AS
                                 (date:chararray, search_context:chararray, search_engine:chararray, search_count:long);
postfltrd = FILTER data BY search_context IS NOT NULL AND 
                           search_engine IS NOT NULL AND
                           search_count IS NOT NULL AND
                           date == '$date';
grpd_searches = GROUP postfltrd BY (date, search_context, search_engine); 
search_counts = FOREACH grpd_searches GENERATE FLATTEN(group) AS (date, search_context, search_engine), 
                                               SUM(postfltrd.search_count) AS search_sum:long;
STORE search_counts INTO 'fhr-search-counts-$date';
/* Store into Vertica (only will work on Vertica 5+ and the vertica connector jar needs to be on every machine)*/
STORE search_counts INTO '{fhr_search_counts(perspective_date date, search_context varchar(32), search_engine varchar(32), count int)}' USING com.vertica.pig.VerticaStorer('$dblist', '$dbname', '$dbport', '$dbuser', '$dbpass');