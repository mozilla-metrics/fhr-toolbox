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

    private static final String CRASHES_FIELD = "org.mozilla.crashes.crashes";
    private static final String CRASHES_PENDING_FIELD = "pending";
    private static final String CRASHES_SUBMITTED_FIELD = "submitted";
    
    private static final String ADDON_COUNTS_FIELD = "org.mozilla.addons.counts";
    private static final String EXTENSION_FIELD = "extension";
    private static final String PLUGIN_FIELD = "plugin";
    private static final String THEME_FIELD = "theme";

    private static final String APPSESSIONS_FIELD = "org.mozilla.appSessions.previous";
    private static final String ABORTED_TOTALTIME_FIELD = "abortedTotalTime";
    	

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
        for (Map.Entry<String, Object> dayEntry : dataPoints.entrySet()) {

            String dayStr = dayEntry.getKey();
            Map<String,Object> dayMap = (Map<String,Object>)dayEntry.getValue();

            if ((dayMap.containsKey(CRASHES_FIELD) || dayMap.containsKey(ADDON_COUNTS_FIELD)) || dayMap.containsKey(APPSESSIONS_FIELD)) {
            	
                // crash info
                Map<String,Object> crashesMap = (Map<String,Object>)dayMap.get(CRASHES_FIELD);
                int crashCountPending = 0, crashCountSubmitted = 0;
				if (crashesMap != null) {
					crashCountPending = getSafeInt(crashesMap.get(CRASHES_PENDING_FIELD));
					crashCountSubmitted = getSafeInt(crashesMap.get(CRASHES_SUBMITTED_FIELD));
				}
                
				// aborted sessions info
                Map<String,Object> sessionsMap = (Map<String,Object>)dayMap.get(APPSESSIONS_FIELD);
                int abortedSessionsCount = 0;
				if (sessionsMap != null) {
					DataBag abortedSessionTimesBag = (DataBag) sessionsMap.get(ABORTED_TOTALTIME_FIELD);
					if (abortedSessionTimesBag != null)
						abortedSessionsCount = (int) abortedSessionTimesBag.size();
				}

                // addons info
                Map<String,Object> addonCountMap = (Map<String,Object>)dayMap.get(ADDON_COUNTS_FIELD);
                int themeCount = 0, extensionCount = 0, pluginCount = 0;
                if (addonCountMap != null) {
                    themeCount = getSafeInt(addonCountMap.get(THEME_FIELD));
                    extensionCount = getSafeInt(addonCountMap.get(EXTENSION_FIELD));
                    pluginCount = getSafeInt(addonCountMap.get(PLUGIN_FIELD));
                }
  
                VersionOnDate vod = new VersionOnDate("yyyy-MM-dd", dayStr);
                final String productVersionOnDate = vod.exec(input);
                
                Tuple t = tupleFactory.newTuple(8);
                t.set(0, dayStr);
                t.set(1, productVersionOnDate);
                t.set(2, crashCountPending);
                t.set(3, crashCountSubmitted);          
                t.set(4, themeCount);
                t.set(5, extensionCount);
                t.set(6, pluginCount);
                t.set(7, abortedSessionsCount);
                dbag.add(t);
            }
        }
        
        return dbag;
    }

}
