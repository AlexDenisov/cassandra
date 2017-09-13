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
    public void testSelectATimestamp() throws Throwable {
        createTable("CREATE TABLE test (key text, v int, PRIMARY KEY (key));");

        execute("INSERT INTO test (key, v) VALUES ('foo', 122) USING TIMESTAMP 22;");

        long timestamp = 22;
        assertRows(execute("SELECT MAX TIMESTAMP test('foo');"),
                   row(timestamp));
    }

    @Test
    public void testSelectMaxTimestamp() throws Throwable {
        createTable("CREATE TABLE test_selectMaxTimestamp (key text, v int, PRIMARY KEY (key));");

        execute("INSERT INTO test_selectMaxTimestamp (key, v) VALUES ('foo', 122) USING TIMESTAMP 22;");
        execute("INSERT INTO test_selectMaxTimestamp (key, v) VALUES ('foo', 101) USING TIMESTAMP 4;");
        execute("INSERT INTO test_selectMaxTimestamp (key, v) VALUES ('foo', 94) USING TIMESTAMP 42;");
        execute("INSERT INTO test_selectMaxTimestamp (key, v) VALUES ('foo', 40) USING TIMESTAMP 16;");

        assertRows(execute("SELECT MAX TIMESTAMP test_selectMaxTimestamp('foo');"),
                   row((long)42));

        execute("INSERT INTO test_selectMaxTimestamp (key, v) VALUES ('bar', 232) USING TIMESTAMP 28;");
        execute("INSERT INTO test_selectMaxTimestamp (key, v) VALUES ('bar', 115) USING TIMESTAMP 57;");
        execute("INSERT INTO test_selectMaxTimestamp (key, v) VALUES ('bar', 12) USING TIMESTAMP 1;");

        assertRows(execute("SELECT MAX TIMESTAMP test_selectMaxTimestamp('bar');"),
                   row((long)57));

        assertRows(execute("SELECT MAX TIMESTAMP test_selectMaxTimestamp('buzz');"),
                   row((long)0));
    }
}

