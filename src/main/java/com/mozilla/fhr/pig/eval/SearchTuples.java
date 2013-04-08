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

	private static final String SEARCHES_KEY = "org.mozilla.searches.counts";

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
		for (Map.Entry<String, Object> dayEntry : dataPoints.entrySet()) {

			String dayStr = dayEntry.getKey();
			Map<String,Object> dayMap = (Map<String,Object>)dayEntry.getValue();

			if (dayMap.containsKey(SEARCHES_KEY)) {

				// search info
				Map<String,Object> searcheCountMap = (Map<String,Object>)dayMap.get(SEARCHES_KEY);
				for (Map.Entry<String, Object> searchEntry : searcheCountMap.entrySet()) {

					// key example: google.searchbar
					String key = searchEntry.getKey();
					long count = ((Number)searchEntry.getValue()).longValue();
					int dotSeparatorPosition = key.indexOf('.');
					if (dotSeparatorPosition != -1 ) {
						String engine = key.substring(0, dotSeparatorPosition);
						String context = key.substring(dotSeparatorPosition+1);
						if (engine.length()!=0 && context.length()!=0) {
							Tuple t = tupleFactory.newTuple(4);
							t.set(0, dayStr);
							t.set(1, context);
							t.set(2, engine);
							t.set(3, count);
							dbag.add(t);
						}
					}
				}
			}
		}

		return dbag;
	}
}
