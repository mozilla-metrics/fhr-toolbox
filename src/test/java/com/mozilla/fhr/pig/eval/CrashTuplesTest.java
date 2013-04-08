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

public class CrashTuplesTest {

	private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	final private int DAY_POSITION_IN_BAG = 0;
	final private int CRASHCOUNT_POSITION_IN_BAG = 1;
	final private int ABORTED_SESSIONS_COUNT_POSITION_IN_BAG = 7;
	
	final private String json = "{\n" + 
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
		"       \"org.mozilla.appInfo.versions\":{ \"_v\":1, \"version\":[ \"21.0a2\" ] },\n" + 
		"       \"org.mozilla.appSessions.previous\":{ \"_v\":3, \"abortedActiveTicks\":[436],\"abortedTotalTime\":[9443] }\n" + 
		"     }\n" + 
		"   }\n" + 
		" },\n" + 
		" \"thisPingDate\":\"2013-03-20\"\n" + 
		"}";

	@SuppressWarnings("unchecked")
	@Test
	public void abortedSessionCountTest() throws IOException {
		Tuple jsonWrapperTuple = tupleFactory.newTuple();
		jsonWrapperTuple.append(json);
		JsonMap jsonMap = new JsonMap();
		Map<String,Object> documentMap = jsonMap.exec(jsonWrapperTuple);
		Map<String,Object> dataMap = (Map<String,Object>)documentMap.get("data");
		Map<String,Object> daysMap = (Map<String,Object>)dataMap.get("days");

		Tuple daysMapWrapperTuple = tupleFactory.newTuple();
		daysMapWrapperTuple.append(daysMap);

		CrashTuples ct = new CrashTuples();
		DataBag bag = ct.exec(daysMapWrapperTuple);

		HashMap<String, Integer> abortedSessonCountOnDay = new HashMap<String, Integer>();
		abortedSessonCountOnDay.put("2013-03-20", 0);
		abortedSessonCountOnDay.put("2013-03-14", 0);
		abortedSessonCountOnDay.put("2013-03-04", 1);

		for (Tuple tuple : bag) {
			String dayStr = (String)tuple.get(DAY_POSITION_IN_BAG);
			Integer abortCount = (Integer)tuple.get(ABORTED_SESSIONS_COUNT_POSITION_IN_BAG);
			assertNotNull("Find day in internal list", abortedSessonCountOnDay.get(dayStr));
			assertEquals("Match aborted session count against internal list for day " + dayStr, abortedSessonCountOnDay.get(dayStr), abortCount);
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void productVersionsInDayMapTest() throws IOException {
		
		Tuple jsonWrapperTuple = tupleFactory.newTuple();
		jsonWrapperTuple.append(json);
		JsonMap jsonMap = new JsonMap();
		Map<String,Object> documentMap = jsonMap.exec(jsonWrapperTuple);
		Map<String,Object> dataMap = (Map<String,Object>)documentMap.get("data");
		Map<String,Object> daysMap = (Map<String,Object>)dataMap.get("days");

		Tuple daysMapWrapperTuple = tupleFactory.newTuple();
		daysMapWrapperTuple.append(daysMap);

		CrashTuples ct = new CrashTuples();
		DataBag bag = ct.exec(daysMapWrapperTuple);

		HashMap<String,String> versionOnDayMap = new HashMap<String, String>();
		versionOnDayMap.put("2013-03-20", "22.0");
		versionOnDayMap.put("2013-03-14", "22.0");
		versionOnDayMap.put("2013-03-04", "21.0a2");

		for (Tuple tuple : bag) {
			String dayStr = (String)tuple.get(DAY_POSITION_IN_BAG);
			String version = (String)tuple.get(CRASHCOUNT_POSITION_IN_BAG);
			assertNotNull("Find in internal version list", versionOnDayMap.get(dayStr));
			assertEquals("Match against internal version list for day " + dayStr, versionOnDayMap.get(dayStr), version);
		}
	}

}
