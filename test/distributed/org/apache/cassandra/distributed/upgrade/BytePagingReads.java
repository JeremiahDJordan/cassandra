/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.distributed.upgrade;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Reads paged in bytes, and the settings they need, run on a node of a mixed-version cluster. Nodes of older releases
 * load this class from the test classpath of the build under test, after their own, so it links against the APIs of
 * DataStax 4.0, 5.0 and 6.0.
 */
public class BytePagingReads
{
    public static SerializableRunnable aggregationSubPageBytes(int bytes)
    {
        return () -> DatabaseDescriptor.setAggregationSubPageSize(PageSize.inBytes(bytes));
    }

    /**
     * The first partition key from the given one, of an int partition key column, whose replicas are the given
     * addresses (host address and ':').
     */
    public static SerializableCallable<Integer> keyWithReplicas(String keyspace, String table, int from, List<String> addresses)
    {
        return () -> {
            for (int key = from; ; key++)
            {
                List<String> replicas = StorageService.instance.getNaturalEndpointsWithPort(keyspace, table, String.valueOf(key));
                if (replicas.size() == addresses.size() && addresses.stream().allMatch(a -> replicas.stream().anyMatch(r -> r.contains(a))))
                    return key;
            }
        };
    }

    /**
     * Reads the query through this node at the given consistency in pages of the given size, in rows or bytes, and
     * returns the rows of each page, their values in hex.
     */
    public static SerializableCallable<List<List<String>>> read(String query, String consistency, int pageSize, boolean inBytes)
    {
        return () -> {
            try
            {
                return readPages(query, consistency, inBytes ? PageSize.inBytes(pageSize) : PageSize.inRows(pageSize));
            }
            catch (ReflectiveOperationException e)
            {
                throw new RuntimeException(e);
            }
        };
    }

    private static List<List<String>> readPages(String query, String consistency, PageSize size) throws ReflectiveOperationException
    {
        CQLStatement statement = QueryProcessor.getStatement(query, ClientState.forInternalCalls());
        // the last argument of execute is the request time, a long on DataStax 4.0
        Method execute = null;
        for (Method method : statement.getClass().getMethods())
        {
            if (method.getName().equals("execute") && method.getParameterCount() == 3
                && method.getParameterTypes()[0] == QueryState.class && method.getParameterTypes()[1] == QueryOptions.class)
                execute = method;
        }
        Class<?> requestTime = execute.getParameterTypes()[2];
        List<List<String>> pages = new ArrayList<>();
        PagingState state = null;
        do
        {
            Object time = requestTime == long.class ? (Object) System.nanoTime() : requestTime.getMethod("forImmediateExecution").invoke(null);
            QueryOptions options = QueryOptions.create(org.apache.cassandra.db.ConsistencyLevel.valueOf(consistency), Collections.emptyList(),
                                                       false, size, state, null, ProtocolVersion.CURRENT, null);
            ResultMessage.Rows rows = (ResultMessage.Rows) execute.invoke(statement, QueryState.forInternalCalls(), options, time);
            List<String> page = new ArrayList<>();
            // the values are byte buffers on DataStax 4.0 and 5.0, byte arrays on 6.0
            for (Object row : (List<?>) rows.result.rows)
            {
                StringBuilder values = new StringBuilder();
                for (Object value : (List<?>) row)
                {
                    values.append(value == null ? "null"
                                                : value instanceof ByteBuffer ? ByteBufferUtil.bytesToHex((ByteBuffer) value)
                                                                              : ByteBufferUtil.bytesToHex(ByteBuffer.wrap((byte[]) value)))
                          .append(' ');
                }
                page.add(values.toString().trim());
            }
            pages.add(page);
            state = rows.result.metadata.getPagingState();
        }
        while (state != null);
        return pages;
    }
}
