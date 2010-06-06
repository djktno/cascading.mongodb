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

package cascading.mongodb;

import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.Mongo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

/**
 * Date: Jun 4, 2010
 * Time: 11:20:26 PM
 */
public class MongoWrapperTest {

    public static final String HOSTNAME = "localhost";
    public static final int PORT = 27017;
    public static final String DATABASE = "testdb";

    public MongoWrapperTest()
    {

    }

    @Before
    public void setup()
    {

    }

    @After
    public void teardown()
    {

    }

    @Test
    public void testMongoWrapper()
    {
        MongoWrapperFactoryFunctor functor = new MongoWrapperFactoryFunctor()
        {
            @Override
            public Mongo makeInstance(String hostname, int port, String database) throws UnknownHostException {
                return new Mongo(new DBAddress(hostname, port, database));
            }
        };

        MongoWrapper.setFactory(functor);
        Mongo m = null;
        Mongo t = null;
        Mongo w = null;

        try {
            m = MongoWrapper.instance(HOSTNAME, PORT, DATABASE);
            t = MongoWrapper.instance(HOSTNAME, PORT, DATABASE);
            w = MongoWrapper.instance();
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        Assert.assertEquals(m, t);
        Assert.assertEquals(m, w);
        Assert.assertEquals(t, w);

        System.out.println(m + ":" + w);

    }

}
