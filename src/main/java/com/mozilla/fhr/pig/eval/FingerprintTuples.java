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


public class FingerprintTuples extends EvalFunc<DataBag> {

    private static final String APPSESSIONS_FIELD = "org.mozilla.appSessions.previous";
    private static final String PROCESS_INIT_TIME_FIELD = "main";
    private static final String FIRST_PAINT_TIME_FIELD = "firstPaint";
    private static final String SESSION_RESTORE_TIME_FIELD = "sessionRestored";

    private static final String PLACES_FIELD = "org.mozilla.places.places";
    private static final String PLACES_BOOKMARKS_FIELD = "bookmarks";
    private static final String PLACES_VISITS_FIELD = "pages";

    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();

    private static int getSafeInt(Object o) {
        if (o == null) {
            return 0;
        }

        return ((Number)o).intValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        
        DataBag dbag = null;

        if (input == null || input.size() == 0) {
            return null;
        }

        try {

            dbag = bagFactory.newDefaultBag();
            Map<String, Object> dataPoints = (Map<String, Object>) input.get(0);
            for (Map.Entry<String, Object> dayEntry : dataPoints.entrySet()) {

                String dayStr = dayEntry.getKey();
                Map<String, Object> dayMap = (Map<String, Object>) dayEntry.getValue();

                if (dayMap.containsKey(APPSESSIONS_FIELD) || dayMap.containsKey(PLACES_FIELD)) {

                    int bookmarkCount = 0;
                    int pagesVisitCount = 0;
                    int processInitTime = 0;
                    int startupTime = 0;
                    int sessionRestoreTime = 0;                

                    // places info
                    Map<String, Object> placesMap = (Map<String, Object>) dayMap.get(PLACES_FIELD);
                    if (placesMap != null) {
                        bookmarkCount = getSafeInt(placesMap.get(PLACES_BOOKMARKS_FIELD));
                        pagesVisitCount = getSafeInt(placesMap.get(PLACES_VISITS_FIELD));
                    }

                    // sessions info
                    Map<String, Object> sessionsMap = (Map<String, Object>) dayMap.get(APPSESSIONS_FIELD);
                    if (sessionsMap != null) {
                        DataBag processInitTimesBag = (DataBag) sessionsMap.get(PROCESS_INIT_TIME_FIELD);
                        DataBag startupTimesBag = (DataBag) sessionsMap.get(FIRST_PAINT_TIME_FIELD);
                        DataBag sessionRestoreTimesBag = (DataBag) sessionsMap.get(SESSION_RESTORE_TIME_FIELD);

                        Tuple processInitTimesTuple = processInitTimesBag.iterator().next();
                        Tuple startupTimesTuple = startupTimesBag.iterator().next();
                        Tuple sessionRestoreTimesTuple = sessionRestoreTimesBag.iterator().next();

                        int startupRecordsInCurrentDay = (int) startupTimesTuple.size();

                        for (int i = 0; i < startupRecordsInCurrentDay; i++) {
                            processInitTime += ((Number) processInitTimesTuple.get(i)).intValue();
                            startupTime += ((Number) startupTimesTuple.get(i)).intValue();
                            sessionRestoreTime += ((Number) sessionRestoreTimesTuple.get(i)).intValue();
                        }
                    }

                    Tuple t = tupleFactory.newTuple(6);
                    t.set(0, dayStr);
                    t.set(1, bookmarkCount);
                    t.set(2, pagesVisitCount);
                    t.set(3, processInitTime);
                    t.set(4, startupTime);
                    t.set(5, sessionRestoreTime);
                    dbag.add(t);
                }
            }

        } catch (Exception e) {
            // ignore
        }

        return dbag;
    }
}
