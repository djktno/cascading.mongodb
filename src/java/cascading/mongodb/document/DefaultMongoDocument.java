/*
 * Copyright (c) 2010 GameAttain, Inc.
 *
 *   This work has been released into the public domain
 *   by the copyright holder. This applies worldwide.
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.mongodb.document;

import cascading.mongodb.MongoDocument;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

import java.util.Iterator;
import java.util.Set;

/**
 * Date: Jun 6, 2010
 * Time: 8:06:39 AM
 */
public class DefaultMongoDocument implements MongoDocument {

    BasicDBObject document = new BasicDBObject();
    Fields selector = new Fields();
    transient TupleEntry tupleEntry = new TupleEntry();

    public DefaultMongoDocument() {

    }

    public DefaultMongoDocument(Fields selectorFields) {
        this.selector = selectorFields;
    }


    public void write(TupleEntry tupleEntry) throws MongoException {


        for (int j = 0; j < selector.size(); j++) {
            Tuple tuple = tupleEntry.selectTuple(selector);

            document.put(selector.get(j).toString(), tuple.getString(j));
        }


    }


    public void readFields(BasicDBObject document) throws MongoException {

        Set<String> keySet = document.keySet();
        if (document.keySet().isEmpty())
            return;

        Fields fields = new Fields();
        Tuple tuple = new Tuple();


        for (Iterator<String> i = keySet.iterator(); i.hasNext();) {
            String key = i.next();
            Object value = document.get(key);

            fields.append(new Fields(key));
            tuple.add(value);
        }

        tupleEntry = new TupleEntry(selector, tuple);

    }

    public BasicDBObject getDocument() {
        return document;
    }

    public void setDocument(BasicDBObject document) throws MongoException {
        this.document = document;
    }

    public TupleEntry getTupleEntry() {
        return tupleEntry;  //To change body of implemented methods use File | Settings | File Templates.
    }


}
