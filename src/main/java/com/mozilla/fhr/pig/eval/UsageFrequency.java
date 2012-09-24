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

import static java.util.Calendar.DATE;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.mozilla.util.DateUtil;

public class UsageFrequency extends EvalFunc<Tuple> {

    public static enum ERRORS { ParseError };
    
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    
    @SuppressWarnings("unchecked")
    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        Tuple output = tupleFactory.newTuple();
        
        List<Long> pingTimes = new ArrayList<Long>();
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
        
        // Calculate the daily difference from ping time to ping time (only 7 most recent pings)
        Collections.sort(pingTimes);
        int start = pingTimes.size() > 7 ? pingTimes.size() - 7 : 0;
        for (int i=start; i < (pingTimes.size()-1); i++) {
            long t1 = pingTimes.get(i);
            long t2 = pingTimes.get(i+1);
            long delta = DateUtil.getTimeDelta(t1, t2, DATE);
            output.append(delta);
        }
        
        return output;
    }

}
