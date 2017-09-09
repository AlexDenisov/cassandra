/*
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
package org.apache.cassandra.cql3;

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;

public class SelectMaxTimestampTest extends CQLTester
{

    @Test
    public void testInvalidRequestException_TableDoesNotExist() throws Throwable {
        assertInvalidThrowMessage("table foo does not exist",
                                  InvalidRequestException.class,
                                  "SELECT MAX TIMESTAMP foo('bar');");

        assertInvalidThrowMessage("keyspace buzz does not exist",
                                  InvalidRequestException.class,
                                  "SELECT MAX TIMESTAMP buzz.foo('bar');");
    }

    @Test
    public void testSelectLatestTimestamp_ReturnsOneRow() throws Throwable {
        execute("CREATE TABLE test (key text, PRIMARY KEY (key));");
        assertRows(execute("SELECT MAX TIMESTAMP test('foo');"),
                   row(42));
    }

    public void testSelectLatestTimestamp() throws Throwable {
        execute("CREATE TABLE test (key text, PRIMARY KEY (key));");

        execute("INSERT INTO test (key) VALUES ('foo') USING TIMESTAMP 22;");
        execute("INSERT INTO test (key) VALUES ('foo') USING TIMESTAMP 14;");

        assertRows(execute("SELECT MAX TIMESTAMP test('foo');"),
                   row(14));

        execute("INSERT INTO test (key) VALUES ('foo') USING TIMESTAMP 18;");

        assertRows(execute("SELECT MAX TIMESTAMP test('foo');"),
                   row(18));
    }
}

