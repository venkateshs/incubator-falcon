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

/**
 * Utility class for checking preconditions of arguments.
 */
public final class Preconditions {

    private Preconditions() {
    }

    /**
     * Check that a string is not null and not empty.
     * If null or empty throws an IllegalArgumentException.
     *
     * @param value value.
     * @param name  parameter name for the exception message.
     */
    public static void notEmpty(String value, String name) {
        if (value == null || value.length() == 0) {
            throw new IllegalArgumentException(name + " cannot be null or empty.");
        }
    }

    /**
     * Check that a string is not null and not empty.
     * If null or empty throws an IllegalArgumentException.
     *
     * @param value value.
     * @param message exception message.
     */
    public static void checkNotEmpty(String value, String message) {
        if (value == null || value.length() == 0) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Check that an object is not null.
     * If null throws an IllegalArgumentException.
     *
     * @param value value.
     * @param name  parameter name for the exception message.
     */
    public static void notNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }
}
