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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.fhr.pig.eval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class LatestPingTimeTest {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException, ParseException {
        Tuple input = tupleFactory.newTuple();
        Map<String,Object> dataPoints = new HashMap<String,Object>();
        dataPoints.put("2012-07-01", "blahblah");
        dataPoints.put("2012-07-02", "blahblah");
        dataPoints.put("2012-07-03", "blahblah");
        dataPoints.put("2012-07-04", "blahblah");
        dataPoints.put("2012-07-05", "blahblah");
        dataPoints.put("2012-07-06", "blahblah");
        dataPoints.put("2012-07-07", "blahblah");
        dataPoints.put("2012-07-09", "blahblah");
        dataPoints.put("2012-07-08", "blahblah");
        input.append(dataPoints);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        LatestPingTime lpt = new LatestPingTime("yyyy-MM-dd", "2012-10-22");
        Long output = lpt.exec(input);
        assertNotNull(output);
        assertEquals(new Long(sdf.parse("2012-07-09").getTime()), output);
    }
    
}
