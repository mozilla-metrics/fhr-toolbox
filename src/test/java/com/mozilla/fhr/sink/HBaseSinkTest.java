package com.mozilla.fhr.sink;
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


import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.mozilla.bagheera.sink.KeyValueSinkFactory;
import com.mozilla.bagheera.sink.SinkConfiguration;

public class HBaseSinkTest {
    SinkConfiguration sinkConfig;
    KeyValueSinkFactory sinkFactory;
    HTablePool hbasePool;
    HTableInterface htable;

    @Before
    public void setup() throws IOException {
        sinkConfig = new SinkConfiguration();
        sinkConfig.setString("hbasesink.hbase.tablename", "test");
        sinkConfig.setString("hbasesink.hbase.column.family", "data");
        sinkConfig.setString("hbasesink.hbase.column.qualifier", "json");
        sinkConfig.setBoolean("hbasesink.hbase.rowkey.prefixdate", false);
        sinkFactory = KeyValueSinkFactory.getInstance(HBaseSink.class, sinkConfig);

        hbasePool = Mockito.mock(HTablePool.class);
        htable = Mockito.mock(HTableInterface.class);

        Mockito.when(hbasePool.getTable("test".getBytes())).thenReturn(htable);

        try {
            Mockito.doAnswer(new Answer<Object>() {
                int count = 0;
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    Object[] result = null;
                    Object[] arguments = invocation.getArguments();
                    List<Row> batchCall = (List<Row>)arguments[0];
                    if (batchCall != null) {
                        result = new Object[batchCall.size()];
                        for (int i = 0; i < result.length; i++) {
                            result[i] = Integer.valueOf(i);
                        }
                    }
                    return result;
                }
            }).when(htable).batch(Mockito.anyListOf(Row.class));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testBatchSize() throws ParseException, IOException {
        HBaseSink sink = (HBaseSink)sinkFactory.getSink("test");
        assertEquals(HBaseSink.DEFAULT_BATCH_SIZE, sink.batchSize);

        sinkConfig.setInt("hbasesink.hbase.batchsize", 5);
        sink = new HBaseSink(sinkConfig);
        assertEquals(5, sink.batchSize);
    }

    @Test
    public void testBatchedDeletes() throws IOException {
        sinkConfig.setInt("hbasesink.hbase.batchsize", 5);
        HBaseSink sink = new HBaseSink(sinkConfig);
        sink.setRetrySleepSeconds(1);
        sink.hbasePool = hbasePool;
        assertEquals(5, sink.batchSize);

        sink.store("req1", "val1".getBytes());
        assertEquals(1, sink.rowQueueSize.get());

        sink.store("req2", "val2".getBytes());
        assertEquals(2, sink.rowQueueSize.get());

        sink.delete("req3");
        assertEquals(3, sink.rowQueueSize.get());
        sink.delete("req4");
        assertEquals(4, sink.rowQueueSize.get());
        sink.store("req5", "val5".getBytes());

        // FLUSH!

        assertEquals(0, sink.rowQueueSize.get());
        sink.delete("req3");
        assertEquals(1, sink.rowQueueSize.get());
    }

}
