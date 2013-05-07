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

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.mozilla.pig.eval.json.JsonMap;

public class SearchTuplesTest {

    final private int DAY_POSITION_IN_BAG = 0;
    final private int CONTEXT_POSITION_IN_BAG = 1;
    final private int ENGINE_POSITION_IN_BAG = 2;
    final private int SEARCH_COUNT_POSITION_IN_BAG = 3;

    String json = "{\n" + 
        " \"version\":1,\n" + 
        " \"data\":{\n" + 
        "   \"days\":{\n" + 
        "     \"2013-03-20\":{\n" + 
        "       \"org.mozilla.addons.counts\":{ \"_v\":1, \"extension\":5, \"plugin\":9, \"theme\":1 },\n" + 
        "       \"org.mozilla.searches.counts\":{ \"_v\":1, \"google.urlbar\":1 }\n" + 
        "     },\n" + 
        "     \"2013-03-14\":{\n" + 
        "       \"org.mozilla.searches.counts\":{ \"_v\":1, \"google.urlbar\":14 },\n" + 
        "       \"org.mozilla.crashes.crashes\":{ \"_v\":1, \"pending\":5 },\n" + 
        "       \"org.mozilla.appInfo.versions\":{ \"_v\":1, \"version\":[ \"22.0\" ] }\n" + 
        "     },\n" + 
        "     \"2013-03-04\":{\n" + 
        "       \"org.mozilla.addons.counts\":{ \"_v\":1, \"extension\":6, \"plugin\":7, \"theme\":1 },\n" + 
        "       \"org.mozilla.searches.counts\":{ \"_v\":1, \"google.abouthome\":1, \"google.urlbar\":18 },\n" + 
        "       \"org.mozilla.appInfo.versions\":{ \"_v\":1, \"version\":[ \"21.0a2\" ] }\n" + 
        "     }\n" + 
        "   }\n" + 
        " },\n" + 
        " \"thisPingDate\":\"2013-03-20\"\n" + 
        "}" ;
    

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @SuppressWarnings("unchecked")
    @Test
    public void crashCountByDayTest() throws IOException {

        Tuple jsonWrapperTuple = tupleFactory.newTuple();
        jsonWrapperTuple.append(json);
        JsonMap jsonMap = new JsonMap();
        Map<String,Object> documentMap = jsonMap.exec(jsonWrapperTuple);
        Map<String,Object> dataMap = (Map<String,Object>)documentMap.get("data");
        Map<String,Object> daysMap = (Map<String,Object>)dataMap.get("days");

        Tuple daysMapWrapperTuple = tupleFactory.newTuple();
        daysMapWrapperTuple.append(daysMap);

        SearchTuples st = new SearchTuples();
        DataBag bag = st.exec(daysMapWrapperTuple);
        
        assertTrue("Result bag must not be empty.", bag.size() > 0);

        HashMap<String, Long> googleUrlbarCountOnDayMap = new HashMap<String, Long>();
        googleUrlbarCountOnDayMap.put("2013-03-20", 1L);
        googleUrlbarCountOnDayMap.put("2013-03-14", 14L);
        googleUrlbarCountOnDayMap.put("2013-03-04", 18L);

        for (Tuple tuple : bag) {
            String dayStr = (String)tuple.get(DAY_POSITION_IN_BAG);
            String context = (String)tuple.get(CONTEXT_POSITION_IN_BAG);
            String engine = (String)tuple.get(ENGINE_POSITION_IN_BAG);
            Long count = (Long)tuple.get(SEARCH_COUNT_POSITION_IN_BAG);
            
            if (engine.equals("google") && context.equals("urlbar")) {
                assertNotNull("Daypoint " + dayStr + " must exist in test internal list", googleUrlbarCountOnDayMap.get(dayStr));
                assertEquals("Match search count against internal version list for day " + dayStr, googleUrlbarCountOnDayMap.get(dayStr), count);
            }
        }
    }


}
