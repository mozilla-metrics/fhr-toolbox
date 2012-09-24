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

import java.io.IOException;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class SearchTuples extends EvalFunc<DataBag> {

    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        DataBag dbag = bagFactory.newDefaultBag();
        Map<String,Object> dataPoints = (Map<String,Object>)input.get(0);
        for (Map.Entry<String, Object> dataPoint : dataPoints.entrySet()) {
            String day = dataPoint.getKey();
            Map<String,Object> fields = (Map<String,Object>)dataPoint.getValue();
            if (fields.containsKey("search")) {
                Map<String,Object> searches = (Map<String,Object>)fields.get("search");
                for (Map.Entry<String, Object> searchMap : searches.entrySet()) {
                    // search context: abouthome, contextmenu, searchbar, urlbar
                    String searchContext = (String)searchMap.getKey();
                    Map<String,Object> searchCountMaps = (Map<String,Object>)searchMap.getValue();
                    for (Map.Entry<String, Object> searchCountMap : searchCountMaps.entrySet()) {
                        Tuple t = tupleFactory.newTuple(4);
                        t.set(0, day);
                        t.set(1, searchContext);
                        t.set(2, searchCountMap.getKey());
                        t.set(3, ((Number)searchCountMap.getValue()).longValue());
                        dbag.add(t);
                    }
                }
            }
        }
        
        return dbag;
    }
}
