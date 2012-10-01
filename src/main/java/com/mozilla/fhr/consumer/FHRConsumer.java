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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

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
        workers = new ArrayList<Future<?>>(streams.size());
        for (final KafkaStream<Message> stream : streams) {  
            workers.add(executor.submit(new Runnable() {
                @Override
                public void run() {                  
                    try {
                        for (MessageAndMetadata<Message> mam : stream) {
                            BagheeraMessage bmsg = BagheeraMessage.parseFrom(ByteString.copyFrom(mam.message().payload()));
                            if (bmsg.getOperation() == Operation.CREATE_UPDATE && 
                                bmsg.hasId() && bmsg.hasPayload()) {
                                ObjectNode document = jsonMapper.readValue(bmsg.getPayload().toStringUtf8(), ObjectNode.class);
                                if (bmsg.hasIpAddr()) {
                                    Country country = geoIpLookupService.getCountry(InetAddress.getByAddress(bmsg.getIpAddr().toByteArray()));
                                    document.put("geo_country", country == null ? "Unknown" : country.getCode());
                                } else {
                                    document.put("geo_country", "Unknown");
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
                }
            }));
        }

        // Wait for all tasks to complete which in the normal case they will
        // run indefinitely unless killed
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.info("Interrupted during polling", e);
        }
    }
    
    public static void main(String[] args) {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = KafkaConsumer.getOptions();
        options.addOption(optFactory.create("tbl", "table", true, "HBase table name.").required());
        options.addOption(optFactory.create("f", "family", true, "Column family."));
        options.addOption(optFactory.create("q", "qualifier", true, "Column qualifier."));
        options.addOption(optFactory.create("pd", "prefixdate", false, "Prefix key with salted date."));
        
        CommandLineParser parser = new GnuParser();
        ShutdownHook sh = ShutdownHook.getInstance();
        try {
            // Parse command line options
            CommandLine cmd = parser.parse(options, args);
            
            final KafkaConsumer consumer = FHRConsumer.fromOptions(cmd);
            sh.addFirst(consumer);
            
            // Create a sink for storing data
            final KeyValueSink sink = new HBaseSink(cmd.getOptionValue("table"), 
                                                    cmd.getOptionValue("family", "data"), 
                                                    cmd.getOptionValue("qualifier", "json"), 
                                                    Boolean.parseBoolean(cmd.getOptionValue("prefixdate", "true")));
            sh.addLast(sink);
            
            // Set the sink for consumer storage
            consumer.setSink(sink);
            
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
