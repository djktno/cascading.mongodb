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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * Date: May 28, 2010
 * Time: 10:21:08 PM
 */
public class CollectionWrapper {

    private Iterator iterator;

    public CollectionWrapper(java.util.Collection<?> collection)
    {
        this.iterator = Collections.unmodifiableCollection(collection).iterator();
    }

    public void forEach(Closure closure)
    {
        while (iterator.hasNext())
        {
            closure.exec(iterator.next());
        }
    }


}
