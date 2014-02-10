/**
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

package org.apache.falcon.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test for Preconditions.
 */
public class PreconditionsTest {

    @Test
    public void testNotEmpty() {
        String value = "foo";
        Preconditions.notEmpty(value, value);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testNotEmptyNegative() {
        String value = "";
        Preconditions.notEmpty(value, value);
    }

    @Test
    public void testCheckNotEmpty() throws Exception {
        String value = "foo";
        Preconditions.checkNotEmpty(value, "error");
    }

    @Test
    public void testCheckNotEmptyNegative() throws Exception {
        String error = "error";
        String value = "";

        try {
            Preconditions.checkNotEmpty(value, error);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), error);
        }
    }

    @Test
    public void testNotNull() throws Exception {
        String name = "foo";

        try {
            Preconditions.notNull(null, name);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), name + " cannot be null");
        }
    }
}
