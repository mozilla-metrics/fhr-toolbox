/*
 * Copyright 2012 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.fhr.consumer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.maxmind.geoip.Country;
import com.maxmind.geoip.LookupService;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage.Operation;
import com.mozilla.bagheera.cli.OptionFactory;
import com.mozilla.bagheera.consumer.KafkaConsumer;
import com.mozilla.bagheera.metrics.MetricsManager;
import com.mozilla.bagheera.sink.HBaseSink;
import com.mozilla.bagheera.sink.KeyValueSink;
import com.mozilla.bagheera.sink.KeyValueSinkFactory;
import com.mozilla.bagheera.sink.SinkConfiguration;
import com.mozilla.bagheera.util.ShutdownHook;

public class FHRConsumer extends KafkaConsumer {

    private static final Logger LOG = Logger.getLogger(FHRConsumer.class);
    
    private ObjectMapper jsonMapper;
    private LookupService geoIpLookupService;
    
    public FHRConsumer(String topic, Properties props) {
        this(topic, props, DEFAULT_NUM_THREADS);
    }

    public FHRConsumer(String topic, Properties props, int numThreads) {
        super(topic, props, numThreads);
        jsonMapper = new ObjectMapper();
        String maxmindPath = props.getProperty("maxmind.db.path");
        try {
            geoIpLookupService = new LookupService(maxmindPath, LookupService.GEOIP_MEMORY_CACHE);
        } catch (IOException e) {
            LOG.error("Failed to load geoip database", e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void close() {
        super.close();
        if (geoIpLookupService != null) {
            geoIpLookupService.close();
        }
    }
    
    @Override
    public void poll() {
        final CountDownLatch latch = new CountDownLatch(streams.size());
        workers = new ArrayList<Future<Void>>(streams.size());
        for (final KafkaStream<Message> stream : streams) {  
            workers.add(executor.submit(new FHRConsumerWorker(stream, latch)));
        }

        // Wait for all tasks to complete which in the normal case they will
        // run indefinitely unless killed
        try {
            while (true) {
                latch.await(10, TimeUnit.SECONDS);
                if (latch.getCount() != streams.size()) {
                    LOG.error("Dead thread detected...exiting.");
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOG.info("Interrupted during polling", e);
        }
        
        // Spit out errors if there were any
        for (Future<Void> worker : workers) {
            try {
                if (worker.isDone()) {
                    worker.get();
                }
            } catch (InterruptedException e) {
                LOG.error("Thread was interrupted:", e);
            } catch (ExecutionException e) {
                LOG.error("Exception occured in thread:", e);
            }
       }
    }
    
    /**
     * This method overrides KafkaConsumer but we can't annotate due to the way Java
     * determines types on static methods.
     */
    public static Options getOptions() {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = KafkaConsumer.getOptions();
        options.addOption(optFactory.create("tbl", "table", true, "HBase table name.").required());
        options.addOption(optFactory.create("f", "family", true, "Column family."));
        options.addOption(optFactory.create("q", "qualifier", true, "Column qualifier."));
        options.addOption(optFactory.create("pd", "prefixdate", false, "Prefix key with salted date."));
        
        return options;
    }
    
    /**
     * This method overrides KafkaConsumer but we can't annotate due to the way Java
     * determines types on static methods.
     */
    public static KafkaConsumer fromOptions(CommandLine cmd) {
        Properties props = new Properties();
        String propsFilePath = cmd.getOptionValue("properties");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(propsFilePath)));
            props.load(reader);
            props.setProperty("groupid", cmd.getOptionValue("groupid"));
        } catch (FileNotFoundException e) {
            LOG.error("Could not find properties file", e);
        } catch (IOException e) {
            LOG.error("Error reading properties file", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOG.error("Error closing properties file", e);
                }
            }
        }
        
        int numThreads = props.containsKey("consumer.threads") ? Integer.parseInt(props.getProperty("consumer.threads")) : DEFAULT_NUM_THREADS;
        // if numthreads specified on command-line then override
        if (cmd.hasOption("numthreads")) {
            numThreads = Integer.parseInt(cmd.getOptionValue("numthreads"));
        }
        
        return new FHRConsumer(cmd.getOptionValue("topic"), props, numThreads);
    }
    
    private class FHRConsumerWorker implements Callable<Void> {
        
        private final KafkaStream<Message> stream;
        private final CountDownLatch latch;
        
        public FHRConsumerWorker(KafkaStream<Message> stream, CountDownLatch latch) {
            this.stream = stream;
            this.latch = latch;
        }
        
        @Override
        public Void call() throws Exception {                  
            try {
                for (MessageAndMetadata<Message> mam : stream) {
                    BagheeraMessage bmsg = BagheeraMessage.parseFrom(ByteString.copyFrom(mam.message().payload()));
                    // get the sink for this message's namespace 
                    // (typically only one sink unless a regex pattern was used to listen to multiple topics)
                    KeyValueSink sink = sinkFactory.getSink(bmsg.getNamespace());
                    if (bmsg.getOperation() == Operation.CREATE_UPDATE && 
                        bmsg.hasId() && bmsg.hasPayload()) {
                        ObjectNode document = jsonMapper.readValue(bmsg.getPayload().toStringUtf8(), ObjectNode.class);
                        if (bmsg.hasIpAddr()) {
                            Country country = geoIpLookupService.getCountry(InetAddress.getByAddress(bmsg.getIpAddr().toByteArray()));
                            document.put("geoCountry", country == null || "--".equals(country.getCode()) ? "Unknown" : country.getCode());
                        } else {
                            document.put("geoCountry", "Unknown");
                        }
                        
                        if (bmsg.hasTimestamp()) {
                            sink.store(bmsg.getId(), jsonMapper.writeValueAsBytes(document), bmsg.getTimestamp());
                        } else {
                            sink.store(bmsg.getId(), jsonMapper.writeValueAsBytes(document));
                        }
                    } else if (bmsg.getOperation() == Operation.DELETE &&
                        bmsg.hasId()) {
                        sink.delete(bmsg.getId());
                    }
                    consumed.mark();
                }
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Invalid protocol buffer in data stream", e);
            } catch (UnsupportedEncodingException e) {
                LOG.error("Message ID was not in UTF-8 encoding", e);
            } catch (IOException e) {
                LOG.error("IO error while storing to data sink", e);
            } finally {
                latch.countDown();
            }
            
            return null;
        }
    }
    
    public static void main(String[] args) {
        Options options = FHRConsumer.getOptions();        
        CommandLineParser parser = new GnuParser();
        ShutdownHook sh = ShutdownHook.getInstance();
        try {
            // Parse command line options
            CommandLine cmd = parser.parse(options, args);
            
            final FHRConsumer consumer = (FHRConsumer)FHRConsumer.fromOptions(cmd);
            sh.addFirst(consumer);
            
            // Set the sink for consumer storage
            SinkConfiguration sinkConfig = new SinkConfiguration();
            if (cmd.hasOption("numthreads")) {
                sinkConfig.setInt("hbasesink.hbase.numthreads", Integer.parseInt(cmd.getOptionValue("numthreads")));
            }
            sinkConfig.setString("hbasesink.hbase.tablename", cmd.getOptionValue("table"));
            sinkConfig.setString("hbasesink.hbase.column.family", cmd.getOptionValue("family", "data"));
            sinkConfig.setString("hbasesink.hbase.column.qualifier", cmd.getOptionValue("qualifier", "json"));
            sinkConfig.setBoolean("hbasesink.hbase.rowkey.prefixdate", Boolean.parseBoolean(cmd.getOptionValue("prefixdate", "true")));
            KeyValueSinkFactory sinkFactory = KeyValueSinkFactory.getInstance(HBaseSink.class, sinkConfig);
            sh.addLast(sinkFactory);
            consumer.setSinkFactory(sinkFactory);
            
            // Initialize metrics collection, reporting, etc.
            MetricsManager.getInstance();
            
            // Begin polling
            consumer.poll();
        } catch (ParseException e) {
            LOG.error("Error parsing command line options", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(FHRConsumer.class.getName(), options);
        }
    }
}
