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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.StorageService;

public class SelectMaxTimestampTest extends CQLTester
{
    @BeforeClass
    public static void initServer()
    {
        StorageService.instance.initServer();
    }

    @Test
    public void testTableOrKeyspaceDoesNotExist() throws Throwable
    {
        assertInvalidThrowMessage("table foo does not exist",
                                  InvalidRequestException.class,
                                  "SELECT MAX TIMESTAMP foo('bar');");

        assertInvalidThrowMessage("keyspace buzz does not exist",
                                  InvalidRequestException.class,
                                  "SELECT MAX TIMESTAMP buzz.foo('bar');");
    }

    @Test
    public void testSelectATimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s(key text, v int, PRIMARY KEY (key));");

        execute("INSERT INTO %s(key, v) VALUES ('foo', 122) USING TIMESTAMP 22;");

        long timestamp = 22;
        System.out.println(currentTable());
        assertRows(execute("SELECT MAX TIMESTAMP %s('foo');"),
                   row(timestamp));
    }

    @Test
    public void testSelectMaxTimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, v int, PRIMARY KEY (key));");

        execute("INSERT INTO %s (key, v) VALUES ('foo', 122) USING TIMESTAMP 22;");
        execute("INSERT INTO %s (key, v) VALUES ('foo', 101) USING TIMESTAMP 4;");
        execute("INSERT INTO %s (key, v) VALUES ('foo', 94) USING TIMESTAMP 42;");
        execute("INSERT INTO %s (key, v) VALUES ('foo', 40) USING TIMESTAMP 16;");

        assertRows(execute("SELECT MAX TIMESTAMP %s('foo');"),
                   row((long)42));

        execute("INSERT INTO %s (key, v) VALUES ('bar', 232) USING TIMESTAMP 28;");
        execute("INSERT INTO %s (key, v) VALUES ('bar', 115) USING TIMESTAMP 57;");
        execute("INSERT INTO %s (key, v) VALUES ('bar', 12) USING TIMESTAMP 1;");

        assertRows(execute("SELECT MAX TIMESTAMP %s('bar');"),
                   row((long)57));

        assertRows(execute("SELECT MAX TIMESTAMP %s('buzz');"),
                   row((long)0));
    }

    @Test
    public void testCompositePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 text, k2 text, v int, PRIMARY KEY ((k1, k2)));");

        execute("INSERT INTO %s (k1, k2, v) VALUES ('foo', 'foo', 122) USING TIMESTAMP 22;");
        execute("INSERT INTO %s (k1, k2, v) VALUES ('foo', 'bar', 101) USING TIMESTAMP 4;");
        execute("INSERT INTO %s (k1, k2, v) VALUES ('bar', 'foo', 94) USING TIMESTAMP 42;");
        execute("INSERT INTO %s (k1, k2, v) VALUES ('bar', 'bar', 98) USING TIMESTAMP 63;");

        assertRows(execute("SELECT MAX TIMESTAMP %s('foo', 'foo');"),
                   row((long)22));
        assertRows(execute("SELECT MAX TIMESTAMP %s('foo', 'bar');"),
                   row((long)4));
        assertRows(execute("SELECT MAX TIMESTAMP %s('bar', 'foo');"),
                   row((long)42));
        assertRows(execute("SELECT MAX TIMESTAMP %s('bar', 'bar');"),
                   row((long)63));

        assertRows(execute("SELECT MAX TIMESTAMP %s('buzz', 'bar');"),
                   row((long)0));
    }

    @Test
    public void testParametersMismatch() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 text, k2 text, v int, PRIMARY KEY ((k1, k2)));");

        String reason = "Supplied parameters do not match: " + currentTable() + " has 2 partition keys, but received 1";
        assertInvalidThrowMessage(reason, InvalidRequestException.class,
                                  "SELECT MAX TIMESTAMP %s('foo');");
    }

    @Test
    public void testTableHasOnlyPrimaryKeys() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, PRIMARY KEY (k));");
        execute("INSERT INTO %s (k) VALUES ('bar') USING TIMESTAMP 99;");
        /// Did not decide yet if this case should throw an exception
        assertRows(execute("SELECT MAX TIMESTAMP %s ('bar');"),
                   row((long)0));
    }

    @Test
    public void testDifferentTimestampAcrossColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, v1 int, v2 int, PRIMARY KEY (k));");
        execute("INSERT INTO %s (k, v1, v2) VALUES ('bar', 14, 22) USING TIMESTAMP 99;");

        assertRows(execute("SELECT MAX TIMESTAMP %s ('bar');"),
                   row((long)99));

        execute("UPDATE %s USING TIMESTAMP 148 SET v1 = 296 WHERE k = 'bar';");

        assertRows(execute("SELECT MAX TIMESTAMP %s ('bar');"),
                   row((long)148));

        execute("UPDATE %s USING TIMESTAMP 220 SET v2 = 104 WHERE k = 'bar';");

        assertRows(execute("SELECT MAX TIMESTAMP %s ('bar');"),
                   row((long)220));
    }

}

