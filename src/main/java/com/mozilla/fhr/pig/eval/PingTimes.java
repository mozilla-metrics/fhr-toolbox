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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PingTimes extends EvalFunc<DataBag> {

    public static enum ERRORS { ParseError };
    
    private static final BagFactory bagFactory = BagFactory.getInstance();
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        Set<Long> pingTimes = new TreeSet<Long>();
        Map<String,Object> dataPoints = (Map<String,Object>)input.get(0);
        for (String dayStr : dataPoints.keySet()) {
            Date pingTime;
            try {
                pingTime = sdf.parse(dayStr);
                pingTimes.add(pingTime.getTime());
            } catch (ParseException e) {
                pigLogger.warn(this, "Parse error parsing pingTime", ERRORS.ParseError);
            }
        }
        
        DataBag db = bagFactory.newDefaultBag();
        for (Long pingTime : pingTimes) {
            Tuple t = tupleFactory.newTuple(1);
            t.set(0, pingTime);
            db.add(t);
        }
        
        return db;
    }
    
}