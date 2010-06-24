/*
 * Copyright (c) 2010 GameAttain, Inc.
 *
 *  This work has been released into the public domain
 *  by the copyright holder. This applies worldwide.
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

package cascading.mongodb;

import org.apache.hadoop.mapred.JobConf;

/**
 * Date: May 24, 2010
 * Time: 9:56:27 PM
 */
public class MongoDBConfiguration {
    private JobConf jobConf;


    /**
     * MongoDB Access URL *
     */
    public static final String DATABASE = "mapred.mongodb.database";

    public static final String COLLECTION = "mapred.mongodb.collection.name";

    public static final String HOST = "mapred.mongodb.host.name"; 

    public static final String PORT = "mapred.mongodb.port";




    public static void configureMongoDB(JobConf jobConf, String database, String collection, String hostname, int port) {

        if (database != null && !"".equals(database))
            jobConf.set(DATABASE, database);

        if (collection != null && !"".equals(collection))
            jobConf.set(COLLECTION, collection);

        jobConf.set(HOST, hostname);

        jobConf.setInt(PORT, port);
    }

    MongoDBConfiguration(JobConf jobConf) {
        this.jobConf = jobConf;
    }


    String getCollection() {
        return jobConf.get(COLLECTION);
    }

    void setCollection(String collection) {
        jobConf.set(COLLECTION, collection);
    }

    String getDatabase() {
        return jobConf.get(DATABASE);
    }

    void setDatabase(String database) {
        jobConf.set(DATABASE, database);
    }

    String getHost() {
        return jobConf.get(HOST);
    }

    void setHost(String host)
    {
        jobConf.set(HOST, host);
    }

    int getPort()
    {
        return jobConf.getInt(PORT, 27017);
    }

    void setPort(int port)
    {
        jobConf.setInt(PORT, port);
    }




}
