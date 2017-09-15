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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SelectMaxTimestampStatement extends CFStatement implements CQLStatement
{
    private final List<Term.Raw> terms;

    public SelectMaxTimestampStatement(CFName name, List<Term.Raw> terms)
    {
        super(name);
        this.terms = terms;
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
        TableMetadata tableMetadata = Schema.instance.validateTable(keyspace(), columnFamily());
        int nowInSec = FBUtilities.nowInSeconds();

        int termsCount = this.terms.size();
        int partitionKeysCount = tableMetadata.partitionKeyColumns().size();

        if (termsCount != partitionKeysCount) {
            String message = "Supplied parameters do not match: " + tableMetadata.name + " has " + partitionKeysCount
                             + " partition keys, but received " + termsCount;
            throw new InvalidRequestException(message);
        }

        ByteBuffer[] buffers = new ByteBuffer[termsCount];
        Iterator<ColumnMetadata> columnMetadataIterator = tableMetadata.partitionKeyColumns().iterator();
        Iterator<Term.Raw> rawTermIterator = terms.iterator();
        for (int i = 0; i < termsCount; i++) {
            ColumnMetadata cm = columnMetadataIterator.next();
            Term.Raw rawTerm = rawTermIterator.next();

            ColumnSpecification specification = new ColumnSpecification(cm.ksName, cm.cfName, cm.name, cm.type);
            Term term = rawTerm.prepare(keyspace(), specification);
            buffers[i] = term.bindAndGet(options);
        }

        ByteBuffer bufferKey = tableMetadata.partitionKeyAsClusteringComparator().make((Object[]) buffers).serializeAsPartitionKey();
        DecoratedKey key = tableMetadata.partitioner.decorateKey(bufferKey);
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(tableMetadata, nowInSec, key);
        PartitionIterator partitionIterator = command.execute(options.getConsistency(), state.getClientState(), queryStartNanoTime);

        ColumnIdentifier identifier = new ColumnIdentifier("MAX TIMESTAMP", false);
        LongType type = LongType.instance;

        ColumnSpecification specification = new ColumnSpecification(keyspace(), columnFamily(), identifier, type);
        List<ColumnSpecification> columnSpecifications = new LinkedList<ColumnSpecification>();
        columnSpecifications.add(specification);
        ResultSet.ResultMetadata metadata = new ResultSet.ResultMetadata(columnSpecifications);
        List<List<ByteBuffer>> rows = new LinkedList<List<ByteBuffer>>();

        long maxTimestamp = 0;
        if (partitionIterator.hasNext())
        {
            RowIterator rowIterator = partitionIterator.next();
            while (rowIterator.hasNext())
            {
                Row row = rowIterator.next();
                Iterator<ColumnMetadata> iterator = row.columns().iterator();
                while (iterator.hasNext()) {
                    ColumnMetadata columnMetadata = iterator.next();
                    Cell cell = row.getCell(columnMetadata);
                    if (cell.timestamp() > maxTimestamp) {
                        maxTimestamp = cell.timestamp();
                    }
                }
            }
        }

        List<ByteBuffer> r = new LinkedList<ByteBuffer>();
        r.add(ByteBufferUtil.bytes(maxTimestamp));
        rows.add(r);
        ResultSet rs = new ResultSet(metadata, rows);
        return new ResultMessage.Rows(rs);
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws InvalidRequestException
    {
        return execute(state, options, System.nanoTime());
    }
}
