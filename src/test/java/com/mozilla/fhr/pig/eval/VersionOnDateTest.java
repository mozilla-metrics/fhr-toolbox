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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class VersionOnDateTest {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException {
        Tuple input = tupleFactory.newTuple();
        DataBag versions = bagFactory.newDefaultBag();
        Tuple v1 = tupleFactory.newTuple(2);
        v1.set(0,"2012-08-01");
        v1.set(1,"14.0");
        versions.add(v1);
        Tuple v2 = tupleFactory.newTuple(2);
        v2.set(0,"2012-09-15");
        v2.set(1,"15.0");
        versions.add(v2);
        input.append(versions);
        
        VersionOnDate vod = new VersionOnDate("yyyy-MM-dd", "2012-10-22");
        String output = vod.exec(input);
        assertTrue("15.0".equals(output));
    }
    
}
