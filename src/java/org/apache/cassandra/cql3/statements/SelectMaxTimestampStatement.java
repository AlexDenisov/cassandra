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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SelectMaxTimestampStatement extends CFStatement implements CQLStatement
{
    private final Term.Raw term;

    public SelectMaxTimestampStatement(CFName name, Term.Raw term)
    {
        super(name);
        this.term = term;
    }

    public int getBoundTerms()
    {
        return 0;
    }

    public Prepared prepare() throws InvalidRequestException
    {
        return new Prepared(this);
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        state.validateLogin();
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException
    {
        Schema.instance.validateTable(keyspace(), columnFamily());

        ColumnIdentifier identifier = new ColumnIdentifier("MAX TIMESTAMP", false);
        Int32Type type = Int32Type.instance;
        ColumnSpecification specification = new ColumnSpecification(keyspace(), columnFamily(), identifier, type);
        List<ColumnSpecification> columnSpecifications = new LinkedList<ColumnSpecification>();
        columnSpecifications.add(specification);
        ResultSet.ResultMetadata metadata = new ResultSet.ResultMetadata(columnSpecifications);
        List<List<ByteBuffer>> rows = new LinkedList<List<ByteBuffer>>();
        List<ByteBuffer> row = new LinkedList<ByteBuffer>();
        row.add(ByteBufferUtil.bytes(42));
        rows.add(row);
        ResultSet rs = new ResultSet(metadata, rows);

        return new ResultMessage.Rows(rs);
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws InvalidRequestException
    {
        return execute(state, options, System.nanoTime());
    }
}
