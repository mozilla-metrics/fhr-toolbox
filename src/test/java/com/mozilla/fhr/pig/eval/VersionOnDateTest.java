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
import java.util.HashMap;
import java.util.Map;

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
        
        Map<String,Object> daysMap = new HashMap<String,Object>();
        Map<String,Object> versionInfoMap1 = new HashMap<String,Object>();
        Map<String,Object> versionMap1 = new HashMap<String,Object>();
        DataBag versionsBag1 = bagFactory.newDefaultBag();
        versionsBag1.add(tupleFactory.newTuple("14.0"));
        versionMap1.put("version", versionsBag1);
        versionInfoMap1.put("org.mozilla.appInfo.versions", versionMap1);
        daysMap.put("2012-08-01", versionInfoMap1);
        
        Map<String,Object> versionInfoMap2 = new HashMap<String,Object>();
        Map<String,Object> versionMap2 = new HashMap<String,Object>();
        DataBag versionsBag2 = bagFactory.newDefaultBag();
        versionsBag2.add(tupleFactory.newTuple("15.0"));
        versionMap2.put("version", versionsBag2);
        versionInfoMap2.put("org.mozilla.appInfo.versions", versionMap2);
        daysMap.put("2012-09-15", versionInfoMap2);

        input.append(daysMap);
        
        VersionOnDate vod = new VersionOnDate("yyyy-MM-dd", "2012-10-22");
        String output = vod.exec(input);
        assertTrue("15.0".equals(output));
    }
    
    @Test
    public void testExec2() throws IOException {
        Tuple input = tupleFactory.newTuple();
        
        Map<String,Object> daysMap = new HashMap<String,Object>();
        Map<String,Object> versionInfoMap1 = new HashMap<String,Object>();
        Map<String,Object> versionMap1 = new HashMap<String,Object>();
        DataBag versionsBag1 = bagFactory.newDefaultBag();
        versionsBag1.add(tupleFactory.newTuple("14.0"));
        versionsBag1.add(tupleFactory.newTuple("15.0a1"));
        versionMap1.put("version", versionsBag1);
        versionInfoMap1.put("org.mozilla.appInfo.versions", versionMap1);
        daysMap.put("2012-08-01", versionInfoMap1);
        
        input.append(daysMap);
        
        VersionOnDate vod = new VersionOnDate("yyyy-MM-dd", "2012-10-22");
        String output = vod.exec(input);
        assertTrue("14.0|15.0a1".equals(output));
    }
    
}
