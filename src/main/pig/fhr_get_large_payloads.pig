register 'akela-0.5-SNAPSHOT.jar'
register 'fhr-toolbox-0.1-SNAPSHOT.jar'
register 'jackson-core-2.1.1.jar'
register 'jackson-databind-2.1.1.jar'
register 'jackson-annotations-2.1.1.jar'

SET pig.logfile fhr_get_large_payloads.log;
/* SET default_parallel 8; */
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

raw = LOAD 'hbase://metrics' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS 
                                   (k:bytearray,json:chararray);
long_payloads = FILTER raw BY SIZE(json) > 1000000;

STORE long_payloads INTO 'fhr_long_payloads_out';
