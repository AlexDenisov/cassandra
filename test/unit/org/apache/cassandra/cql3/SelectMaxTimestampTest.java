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

    @Test
    public void testSelectMaxTimestamp_CompositePartitionKey() throws Throwable {
        createTable("CREATE TABLE test_selectMaxTimestamp_CompositePartitionKey (k1 text, k2 text, v int, PRIMARY KEY ((k1, k2)));");

        execute("INSERT INTO test_selectMaxTimestamp_CompositePartitionKey (k1, k2, v) VALUES ('foo', 'foo', 122) USING TIMESTAMP 22;");
        execute("INSERT INTO test_selectMaxTimestamp_CompositePartitionKey (k1, k2, v) VALUES ('foo', 'bar', 101) USING TIMESTAMP 4;");
        execute("INSERT INTO test_selectMaxTimestamp_CompositePartitionKey (k1, k2, v) VALUES ('bar', 'foo', 94) USING TIMESTAMP 42;");
        execute("INSERT INTO test_selectMaxTimestamp_CompositePartitionKey (k1, k2, v) VALUES ('bar', 'bar', 98) USING TIMESTAMP 63;");

        assertRows(execute("SELECT MAX TIMESTAMP test_selectMaxTimestamp_CompositePartitionKey('foo', 'foo');"),
                   row((long)22));
        assertRows(execute("SELECT MAX TIMESTAMP test_selectMaxTimestamp_CompositePartitionKey('foo', 'bar');"),
                   row((long)4));
        assertRows(execute("SELECT MAX TIMESTAMP test_selectMaxTimestamp_CompositePartitionKey('bar', 'foo');"),
                   row((long)42));
        assertRows(execute("SELECT MAX TIMESTAMP test_selectMaxTimestamp_CompositePartitionKey('bar', 'bar');"),
                   row((long)63));

        assertRows(execute("SELECT MAX TIMESTAMP test_selectMaxTimestamp_CompositePartitionKey('buzz', 'bar');"),
                   row((long)0));
    }

    @Test
    public void testSelectMaxTimestamp_ParametersMismatch() throws Throwable {
        createTable("CREATE TABLE testSelectMaxTimestamp_ParametersMismatch (k1 text, k2 text, v int, PRIMARY KEY ((k1, k2)));");

        /// Note: cfName is getting lowercased, because of that the error message is not very user-friendly
        assertInvalidThrowMessage("Supplied parameters do not match: testselectmaxtimestamp_parametersmismatch has 2 partition keys, but received 1",
                                  InvalidRequestException.class,
                                  "SELECT MAX TIMESTAMP testSelectMaxTimestamp_ParametersMismatch('foo');");
    }

    @Test
    public void testSelectMaxTimestamp_TableHasOnlyPrimaryKeys() throws Throwable {
        createTable("CREATE TABLE testSelectMaxTimestamp_TableHasOnlyPrimaryKeys (k text, PRIMARY KEY (k));");
        execute("INSERT INTO testSelectMaxTimestamp_TableHasOnlyPrimaryKeys (k) VALUES ('bar') USING TIMESTAMP 99;");
        /// Did not decide yet if this case should throw an exception
        assertRows(execute("SELECT MAX TIMESTAMP testSelectMaxTimestamp_TableHasOnlyPrimaryKeys('bar');"),
                   row((long)0));
    }

}

