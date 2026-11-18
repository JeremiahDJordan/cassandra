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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.schema.DroppedColumn;

public class DeserializationHelper
{
    /**
     * Flag affecting deserialization behavior (this only affect counters in practice).
     *  - LOCAL: for deserialization of local data (Expired columns are
     *      converted to tombstones (to gain disk space)).
     *  - FROM_REMOTE: for deserialization of data received from remote hosts
     *      (Expired columns are converted to tombstone and counters have
     *      their delta cleared)
     *  - PRESERVE_SIZE: used when no transformation must be performed, i.e,
     *      when we must ensure that deserializing and reserializing the
     *      result yield the exact same bytes. Streaming uses this.
     */
    public enum Flag
    {
        LOCAL, FROM_REMOTE, PRESERVE_SIZE
    }

    private final Flag flag;
    public final int version;

    private final ColumnFilter columnsToFetch;
    private ColumnFilter.Tester tester;

    private final boolean dropsNotQueriedCellsOlderThanRow;

    private final boolean hasDroppedColumns;
    private final Map<ByteBuffer, DroppedColumn> droppedColumns;
    private DroppedColumn currentDroppedComplex;


    public DeserializationHelper(TableMetadata metadata, int version, Flag flag, ColumnFilter columnsToFetch)
    {
        this.flag = flag;
        this.version = version;
        this.columnsToFetch = columnsToFetch;
        this.dropsNotQueriedCellsOlderThanRow = columnsToFetch != null && columnsToFetch.dropsNotQueriedCellsOlderThanRow();
        this.droppedColumns = metadata.droppedColumns;
        this.hasDroppedColumns = droppedColumns.size() > 0;
    }

    public DeserializationHelper(TableMetadata metadata, int version, Flag flag)
    {
        this(metadata, version, flag, null);
    }

    public boolean includes(ColumnMetadata column)
    {
        return columnsToFetch == null || columnsToFetch.fetches(column);
    }

    public boolean includes(Cell<?> cell, LivenessInfo rowLiveness)
    {
        // During queries, some columns are included even though they are not queried by the user because
        // we always need to distinguish between having a row (with potentially only null values) and not
        // having a row at all (see #CASSANDRA-7085 for background). The values of the cells of those columns
        // are skipped (canSkipValue). The cells older than the primary key liveness info of the row are removed
        // once the sources are merged (see ReadCommand#withoutSkippableCells), as that info may not be live, or may
        // be superseded by one from another source that is not live, and those cells are then what keeps the row
        // alive. The simple ones are only removed here when the filter allows it
        // (ColumnFilter#withNotQueriedCellsOlderThanRowDropped).
        if (columnsToFetch == null)
            return true;

        ColumnMetadata column = cell.column();
        if (column.isComplex())
            return includes(cell.path());

        return !dropsNotQueriedCellsOlderThanRow
               || cell.timestamp() >= rowLiveness.timestamp()
               || columnsToFetch.fetchedColumnIsQueried(column);
    }

    public boolean includes(CellPath path)
    {
        return path == null || tester == null || tester.fetches(path);
    }

    public boolean canSkipValue(ColumnMetadata column)
    {
        return columnsToFetch != null && !columnsToFetch.fetchedColumnIsQueried(column);
    }

    public boolean canSkipValue(CellPath path)
    {
        return path != null && tester != null && !tester.fetchedCellIsQueried(path);
    }

    public void startOfComplexColumn(ColumnMetadata column)
    {
        this.tester = columnsToFetch == null ? null : columnsToFetch.newTester(column);
        this.currentDroppedComplex = droppedColumns.get(column.name.bytes);
    }

    public void endOfComplexColumn()
    {
        this.tester = null;
    }

    public boolean isDropped(Cell<?> cell, boolean isComplex)
    {
        if (!hasDroppedColumns)
            return false;

        DroppedColumn dropped = isComplex ? currentDroppedComplex : droppedColumns.get(cell.column().name.bytes);
        return dropped != null && cell.timestamp() <= dropped.droppedTime;
    }

    public boolean isDroppedComplexDeletion(DeletionTime complexDeletion)
    {
        return currentDroppedComplex != null && complexDeletion.markedForDeleteAt() <= currentDroppedComplex.droppedTime;
    }

    public <V> V maybeClearCounterValue(V value, ValueAccessor<V> accessor)
    {
        return flag == Flag.FROM_REMOTE || (flag == Flag.LOCAL && CounterContext.instance().shouldClearLocal(value, accessor))
               ? CounterContext.instance().clearAllLocal(value, accessor)
               : value;
    }
}
