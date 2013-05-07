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

public class StartupTimesTuples extends EvalFunc<DataBag> {

	private static final String APPSESSIONS_FIELD = "org.mozilla.appSessions.previous";
	private static final String PROCESS_INIT_TIME_FIELD = "main";
	private static final String FIRST_PAINT_TIME_FIELD = "firstPaint";
	private static final String SESSION_RESTORE_TIME_FIELD = "sessionRestored";

	private static BagFactory bagFactory = BagFactory.getInstance();
	private static TupleFactory tupleFactory = TupleFactory.getInstance();

	@SuppressWarnings("unchecked")
	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}

		DataBag dbag = bagFactory.newDefaultBag();
		Map<String, Object> dataPoints = (Map<String, Object>) input.get(0);
		for (Map.Entry<String, Object> dayEntry : dataPoints.entrySet()) {

			String dayStr = dayEntry.getKey();
			Map<String, Object> dayMap = (Map<String, Object>) dayEntry.getValue();

			if (dayMap.containsKey(APPSESSIONS_FIELD)) {
				Integer startupRecordsInCurrentDay = null;
				
                VersionOnDate vod = new VersionOnDate("yyyy-MM-dd", dayStr);
                final String productVersionOnDate = vod.exec(input);

				// sessions info
				Map<String, Object> sessionsMap = (Map<String, Object>) dayMap.get(APPSESSIONS_FIELD);
				if (sessionsMap != null) {
					DataBag processInitTimesBag = (DataBag) sessionsMap.get(PROCESS_INIT_TIME_FIELD);
					DataBag startupTimesBag = (DataBag) sessionsMap.get(FIRST_PAINT_TIME_FIELD);
					DataBag sessionRestoreTimesBag = (DataBag) sessionsMap.get(SESSION_RESTORE_TIME_FIELD);

					Tuple processInitTimesTuple = processInitTimesBag.iterator().next();
					Tuple startupTimesTuple = startupTimesBag.iterator().next();
					Tuple sessionRestoreTimesTuple = sessionRestoreTimesBag.iterator().next();
					
					if (startupRecordsInCurrentDay == null)
						startupRecordsInCurrentDay = (int) startupTimesTuple.size();

					for (int i = 0; i < startupRecordsInCurrentDay; i++) {
						Tuple t = tupleFactory.newTuple(5);
						t.set(0, dayStr);
						t.set(1, productVersionOnDate);
						t.set(2, processInitTimesTuple.get(i));
						t.set(3, startupTimesTuple.get(i));
						t.set(4, sessionRestoreTimesTuple.get(i));
						dbag.add(t);
					}
				}
			}
		}

		return dbag;
	}
}
