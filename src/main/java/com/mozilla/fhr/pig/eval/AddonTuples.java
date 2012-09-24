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

public class AddonTuples extends EvalFunc<DataBag> {

    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        DataBag dbag = bagFactory.newDefaultBag();
        
        int size = input.size();
        for (int i=0; i < size; i++) {
            Map<String,Object> addonMap = (Map<String,Object>)input.get(i);
            if (addonMap != null) {
                Tuple t = tupleFactory.newTuple(8);
                t.set(0, addonMap.get("id"));
                t.set(1, addonMap.get("userDisabled"));
                t.set(2, addonMap.get("appDisabled"));
                t.set(3, addonMap.get("version"));
                t.set(4, addonMap.get("type"));
                t.set(5, addonMap.get("hasBinaryComponents"));
                t.set(6, addonMap.get("installDate"));
                t.set(7, addonMap.get("updateDate"));
                dbag.add(t);
            }
        }
        
        return dbag;
    }

}
