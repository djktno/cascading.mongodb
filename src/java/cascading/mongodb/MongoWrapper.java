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

/**
 * Date: Jun 4, 2010
 * Time: 2:25:33 PM
 */
package cascading.mongodb;

import com.mongodb.DBAddress;
import com.mongodb.Mongo;

import java.net.UnknownHostException;

public class MongoWrapper {

    static private MongoWrapperFactoryFunctor _factory = null;

    static private Mongo _instance = null;

    static private Mongo makeInstance(String hostname, int port, String database) throws UnknownHostException
    {
        return new Mongo(new DBAddress(hostname, port, database));
    }

    static private Mongo makeInstance() throws UnknownHostException
    {
        return new Mongo();
    }

    static public synchronized Mongo instance() throws UnknownHostException
    {
        if (null == _instance)
        {
            _instance = makeInstance();
        }
        return _instance;
    }

    static public synchronized Mongo instance(String hostname, int port, String database) throws UnknownHostException
    {
        if (null == _instance)
        {
            _instance = (null == _factory) ? makeInstance(hostname, port, database) : _factory.makeInstance(hostname, port, database);
        }
        return _instance;
    }

    static public synchronized void setFactory(MongoWrapperFactoryFunctor factory)
    {
        _factory = factory;
    }

    static public synchronized void setInstance(Mongo instance)
    {
        _instance = instance;
    }
    

}
