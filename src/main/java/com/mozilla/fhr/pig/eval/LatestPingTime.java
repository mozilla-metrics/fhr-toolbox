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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


/**
 * Gets the latest ping time from data points that is less than or equal to the perspective date.
 */
public class LatestPingTime extends EvalFunc<Long> {

    public static enum ERRORS { ParseError };
    
    private final SimpleDateFormat sdf;
    private long perspectiveTime;
    
    public LatestPingTime(String dateFormat, String perspectiveDate) {
        sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date d = sdf.parse(perspectiveDate);
            perspectiveTime = d.getTime();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid perspective date", e);
        }        
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Long exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
         
        Long maxPingTime = null;
        Map<String,Object> dataPoints = (Map<String,Object>)input.get(0);
        for (String dayStr : dataPoints.keySet()) {
            try {
                Date pingTime = sdf.parse(dayStr);
                if (maxPingTime == null || (pingTime.getTime() <= perspectiveTime && pingTime.getTime() > maxPingTime)) {
                    maxPingTime = pingTime.getTime();
                }
            } catch (ParseException e) {
                pigLogger.warn(this, "Parse error parsing pingTime", ERRORS.ParseError);
            }
        }
        
        return maxPingTime;
    }
    
}