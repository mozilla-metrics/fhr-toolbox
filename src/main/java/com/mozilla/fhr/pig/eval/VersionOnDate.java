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
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class VersionOnDate extends EvalFunc<String> {

    public static enum ERRORS { ParseError };
    
    private final SimpleDateFormat sdf;
    private long perspectiveTime;
    
    public VersionOnDate(String dateFormat, String perspectiveDate) {
        sdf = new SimpleDateFormat(dateFormat);
        try {
            Date d = sdf.parse(perspectiveDate);
            perspectiveTime = d.getTime();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid perspective date", e);
        }
    }
    
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        String latestVersion = null;
        long latestTime = 0;
        try {
            DataBag versions = (DataBag)input.get(0);
            Iterator<Tuple> iter = versions.iterator();
            while (iter.hasNext()) {
                Tuple t = iter.next();
                Date d = sdf.parse((String)t.get(0));
                if (d.getTime() <= perspectiveTime && d.getTime() > latestTime) {
                    latestVersion = (String)t.get(1);
                }
            }
        } catch (ParseException e) {
            pigLogger.warn(this, "Error parsing versions date", ERRORS.ParseError);
        }
        
        return latestVersion;
    }

    
}
