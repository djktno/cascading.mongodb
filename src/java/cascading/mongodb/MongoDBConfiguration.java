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

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import org.apache.hadoop.mapred.JobConf;

import java.net.UnknownHostException;

/**
 * Date: May 24, 2010
 * Time: 9:56:27 PM
 */
public class MongoDBConfiguration
{
    private JobConf jobConf;
    private Mongo m;


    /** MongoDB Access URL **/
    public static final String URL_PROPERTY = "mapred.mongodb.url";

    public static final String HOSTNAME = "mapred.mongodb.host";

    public static final String PORT = "mapred.mongodb.port";

    private static final int DEFAULT_PORT = 27017;

    public static final String USERNAME = "mapred.mongodb.username";

    public static final String PASSWORD = "mapred.mongodb.password";

    public static final String DATABASE = "mapred.mongodb.database";

    public static final String OUTPUT_COLLECTION = "mapred.mongodb.output.collection.name";

    public static final String OUTPUT_DOCUMENT_ATTRIBUTE_NAMES = "mapred.mongodb.output.document.attribute.names";

    public static void configureMongoDB(JobConf jobConf, String url, String username, String password)
    {
       configureMongoDB(jobConf, url, username, password, null);
        
    }

    public static void configureMongoDB(JobConf jobConf, String url, String username, String password, String database)
    {
       jobConf.set(URL_PROPERTY, url);

        if (username != null && !"".equals(username))
            jobConf.set(USERNAME, username);

        if (password != null && !"".equals(password))
            jobConf.set(PASSWORD, password);

        if (database != null && !"".equals(database))
            jobConf.set(DATABASE, database);
    }

    public static void configureMongoDB(JobConf jobConf, String hostname, int port, String username, String password)
    {
       configureMongoDB(jobConf, hostname, port, username, password, null);

    }

    public static void configureMongoDB(JobConf jobConf, String hostname, int port, String username, String password, String database)
    {
       jobConf.set(HOSTNAME, hostname);

        if (port != 0)
            jobConf.setInt(PORT, port);
        else
            jobConf.setInt(PORT, DEFAULT_PORT);

        if (username != null && !"".equals(username))
            jobConf.set(USERNAME, username);

        if (password != null && !"".equals(password))
            jobConf.set(PASSWORD, password);

        if (database != null && !"".equals(database))
            jobConf.set(DATABASE, database);
    }

    MongoDBConfiguration(JobConf jobConf)
    {
        this.jobConf = jobConf;
    }

    DB getDB() throws MongoException
    {
        if (m == null)
            try
            {
                m = new Mongo(getHostname(), getPort());
            }
            catch (UnknownHostException e)
            {
                throw new MongoException("Unknown host {hostname: " + jobConf.get(HOSTNAME) + "}");
            }

        return m.getDB(getDatabase());
    }

    

    String getCollection()
    {
        return jobConf.get(OUTPUT_COLLECTION);
    }

    void setCollection(String collection)
    {
        jobConf.set(OUTPUT_COLLECTION, collection);
    }

    String getDatabase()
    {
        return jobConf.get(DATABASE);
    }

    void setDatabase(String database)
    {
        jobConf.set(DATABASE, database);
    }

    String[] getDocumentAttributeNames()
    {
        return jobConf.getStrings(OUTPUT_DOCUMENT_ATTRIBUTE_NAMES);
    }

    void setDocumentAttributeNames(String... attributeNames)
    {
        jobConf.setStrings(OUTPUT_DOCUMENT_ATTRIBUTE_NAMES, attributeNames);
    }

    String getHostname()
    {
        return jobConf.get(HOSTNAME, "localhost");
    }

    void setHostname(String hostname)
    {
        jobConf.set(HOSTNAME, hostname);
    }

    int getPort()
    {
        return jobConf.getInt(PORT, DEFAULT_PORT);
    }

    void setPort(int port)
    {
        jobConf.setInt(PORT, port);
    }
    
}
