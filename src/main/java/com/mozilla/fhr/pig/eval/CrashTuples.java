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

public class CrashTuples extends EvalFunc<DataBag> {

    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();

    private int getSafeInt(Object o) {
        if (o == null) {
            return 0;
        }
        
        return ((Number)o).intValue();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        DataBag dbag = bagFactory.newDefaultBag();
        Map<String,Object> dataPoints = (Map<String,Object>)input.get(0);
        for (Map.Entry<String, Object> dataPoint : dataPoints.entrySet()) {
            String dayStr = dataPoint.getKey();
            Map<String,Object> fields = (Map<String,Object>)dataPoint.getValue();
            if ((fields.containsKey("crashCountPending") || fields.containsKey("crashCountSubmitted")) && fields.containsKey("sessions")) {
                // crash info
                int crashCountPending = getSafeInt(fields.get("crashCountPending"));
                int crashCountSubmitted = getSafeInt(fields.get("crashCountSubmitted"));
                
                // aborted session info
                Map<String,Object> sessions = (Map<String,Object>)fields.get("sessions");
                int aborted = getSafeInt(sessions.get("aborted"));
                int abortedTime = getSafeInt(sessions.get("abortedTime"));
                int abortedActiveTime = getSafeInt(sessions.get("abortedActiveTime"));
                
                // addons info
                Map<String,Object> addonCounts = (Map<String,Object>)fields.get("addonCounts");
                int themeCount = 0, extCount = 0, pluginCount = 0;
                if (addonCounts != null) {
                    themeCount = getSafeInt(addonCounts.get("theme"));
                    extCount = getSafeInt(addonCounts.get("extension"));
                    pluginCount = getSafeInt(addonCounts.get("plugin"));
                }
                
                Tuple t = tupleFactory.newTuple(9);
                t.set(0, dayStr);
                t.set(1, crashCountPending);
                t.set(2, crashCountSubmitted);          
                t.set(3, aborted);
                t.set(4, abortedTime);
                t.set(5, abortedActiveTime);
                t.set(6, themeCount);
                t.set(7, extCount);
                t.set(8, pluginCount);
                
                dbag.add(t);
            }
        }

        
        return dbag;
    }

}
