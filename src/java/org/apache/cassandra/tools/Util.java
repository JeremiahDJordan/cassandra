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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.LongFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidColumnTypeException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;

import static java.lang.String.format;

@SuppressWarnings("serial")
public final class Util
{
    static final String RESET = "\u001B[0m";
    static final String BLUE = "\u001B[34m";
    static final String CYAN = "\u001B[36m";
    static final String WHITE = "\u001B[37m";
    private static final List<String> ANSI_COLORS = Lists.newArrayList(RESET, BLUE, CYAN, WHITE);

    private static final String FULL_BAR_UNICODE = Strings.repeat("\u2593", 30);
    private static final String EMPTY_BAR_UNICODE = Strings.repeat("\u2591", 30);
    private static final String FULL_BAR_ASCII = Strings.repeat("#", 30);
    private static final String EMPTY_BAR_ASCII = Strings.repeat("-", 30);

    private static final TreeMap<Double, String> BARS_UNICODE = new TreeMap<Double, String>()
    {{
        this.put(1.0,       "\u2589"); // full, actually using 7/8th for bad font impls of fullblock
        this.put(7.0 / 8.0, "\u2589"); // 7/8ths left block
        this.put(3.0 / 4.0, "\u258A"); // 3/4th block
        this.put(5.0 / 8.0, "\u258B"); // 5/8th
        this.put(3.0 / 8.0, "\u258D"); // three eighths, skips 1/2 due to font inconsistencies
        this.put(1.0 / 4.0, "\u258E"); // 1/4th
        this.put(1.0 / 8.0, "\u258F"); // 1/8th
    }};

    private static final TreeMap<Double, String> BARS_ASCII = new TreeMap<Double, String>()
    {{
        this.put(1.00, "O");
        this.put(0.75, "o");
        this.put(0.30, ".");
    }};

    private static TreeMap<Double, String> barmap(boolean unicode)
    {
        return unicode ? BARS_UNICODE : BARS_ASCII;
    }

    public static String progress(double percentComplete, int width, boolean unicode)
    {
        assert percentComplete >= 0 && percentComplete <= 1;
        int cols = (int) (percentComplete * width);
        return (unicode ? FULL_BAR_UNICODE : FULL_BAR_ASCII).substring(width - cols) +
               (unicode ? EMPTY_BAR_UNICODE : EMPTY_BAR_ASCII ).substring(cols);
    }

    public static String stripANSI(String string)
    {
        return ANSI_COLORS.stream().reduce(string, (a, b) -> a.replace(b, ""));
    }

    public static int countANSI(String string)
    {
        return string.length() - stripANSI(string).length();
    }

    public static String wrapQuiet(String toWrap, boolean color)
    {
        if (Strings.isNullOrEmpty(toWrap))
        {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        if (color) sb.append(WHITE);
        sb.append("(");
        sb.append(toWrap);
        sb.append(")");
        if (color) sb.append(RESET);
        return sb.toString();
    }

    public static class TermHistogram
    {
        public long max;
        public long min;
        public double sum;
        int maxCountLength = 5;
        int maxOffsetLength = 5;
        Map<? extends Number, Long> histogram;
        LongFunction<String> offsetName;
        LongFunction<String> countName;
        String title;

        public TermHistogram(Map<? extends Number, Long> histogram,
                String title,
                LongFunction<String> offsetName,
                LongFunction<String> countName)
        {
            this.offsetName = offsetName;
            this.countName = countName;
            this.histogram = histogram;
            this.title = title;
            maxOffsetLength = title.length();
            histogram.entrySet().stream().forEach(e ->
            {
                max = Math.max(max, e.getValue());
                min = Math.min(min, e.getValue());
                sum += e.getValue();
                // find max width, but remove ansi sequences first
                maxCountLength = Math.max(maxCountLength, stripANSI(countName.apply(e.getValue())).length());
                maxOffsetLength = Math.max(maxOffsetLength, stripANSI(offsetName.apply(e.getKey().longValue())).length());
            });
        }

        public TermHistogram(TombstoneHistogram histogram,
                String title,
                LongFunction<String> offsetName,
                LongFunction<String> countName)
        {
            this(new TreeMap<Number, Long>()
            {
                {
                    histogram.forEach((point, value) -> {
                        this.put(point, (long) value);
                    });
                }
            }, title, offsetName, countName);
        }

        public TermHistogram(EstimatedHistogram histogram,
                String title,
                LongFunction<String> offsetName,
                LongFunction<String> countName)
        {
            this(new TreeMap<Number, Long>()
            {
                {
                    long[] counts = histogram.getBuckets(false);
                    long[] offsets = histogram.getBucketOffsets();
                    for (int i = 0; i < counts.length; i++)
                    {
                        long e = counts[i];
                        if (e > 0)
                        {
                            put(offsets[i], e);
                        }
                    }
                }
            }, title, offsetName, countName);
        }

        public String bar(long count, int length, String color, boolean unicode)
        {
            if (color == null) color = "";
            StringBuilder sb = new StringBuilder(color);
            long barVal = count;
            int intWidth = (int) (barVal * 1.0 / max * length);
            double remainderWidth = (barVal * 1.0 / max * length) - intWidth;
            sb.append(Strings.repeat(barmap(unicode).get(1.0), intWidth));

            if (barmap(unicode).floorKey(remainderWidth) != null)
                sb.append(barmap(unicode).get(barmap(unicode).floorKey(remainderWidth)));

            if(!Strings.isNullOrEmpty(color))
                sb.append(RESET);

            return sb.toString();
        }

        public void printHistogram(PrintStream out, boolean color, boolean unicode)
        {
            // String.format includes ansi sequences in the count, so need to modify the lengths
            int offsetTitleLength = color ? maxOffsetLength + BLUE.length() : maxOffsetLength;
            out.printf("   %-" + offsetTitleLength + "s %s %-" + maxCountLength + "s  %s  %sHistogram%s %n",
                       color ? BLUE + title : title,
                       color ? CYAN + "|" + BLUE : "|",
                       "Count",
                       wrapQuiet("%", color),
                       color ? BLUE : "",
                       color ? RESET : "");
            histogram.entrySet().stream().forEach(e ->
            {
                String offset = offsetName.apply(e.getKey().longValue());
                long count = e.getValue();
                String histo = bar(count, 30, color? WHITE : null, unicode);
                int mol = color ? maxOffsetLength + countANSI(offset) : maxOffsetLength;
                int mcl = color ? maxCountLength + countANSI(countName.apply(count)) : maxCountLength;
                out.printf("   %-" + mol + "s %s %" + mcl + "s %s %s%n",
                           offset,
                           color ? CYAN + "|" + RESET : "|",
                           countName.apply(count),
                           wrapQuiet(String.format("%3s", (int) (100 * ((double) count / sum))), color),
                           histo);
            });
            EstimatedHistogram eh = new EstimatedHistogram(165);
            for (Entry<? extends Number, Long> e : histogram.entrySet())
            {
                eh.add(e.getKey().longValue(), e.getValue());
            }
            String[] percentiles = new String[]{"50th", "75th", "95th", "98th", "99th", "Min", "Max"};
            long[] data = new long[]
            {
                eh.percentile(.5),
                eh.percentile(.75),
                eh.percentile(.95),
                eh.percentile(.98),
                eh.percentile(.99),
                eh.min(),
                eh.max(),
            };
            out.println((color ? BLUE : "") + "   Percentiles" + (color ? RESET : ""));

            for (int i = 0; i < percentiles.length; i++)
            {
                out.println(format("   %s%-10s%s%s",
                                   (color ? BLUE : ""),
                                   percentiles[i],
                                   (color ? RESET : ""),
                                   offsetName.apply(data[i])));
            }
        }
    }
    private Util()
    {
    }

    /**
     * This is used by standalone tools to force static initialization of DatabaseDescriptor, and fail if configuration
     * is bad.
     */
    public static void initDatabaseDescriptor()
    {
        try
        {
            DatabaseDescriptor.toolInitialization();
        }
        catch (Throwable e)
        {
            boolean logStackTrace = !(e instanceof ConfigurationException) || ((ConfigurationException) e).logStackTrace;
            System.out.println("Exception (" + e.getClass().getName() + ") encountered during startup: " + e.getMessage());

            if (logStackTrace)
            {
                e.printStackTrace();
                System.exit(3);
            }
            else
            {
                System.err.println(e.getMessage());
                System.exit(3);
            }
        }
    }

    public static <T> Stream<T> iterToStream(Iterator<T> iter)
    {
        Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
    }

    /**
     * Construct table schema from info stored in SSTable's Stats.db.
     * Hardcodes the keyspace and table name to default values to preserve the existing behavior.
     *
     * @param desc SSTable's descriptor
     * @return Restored CFMetaData
     * @throws IOException when Stats.db cannot be read
     */
    public static TableMetadata metadataFromSSTable(Descriptor desc) throws IOException
    {
        return metadataFromSSTable(desc, "keyspace", "table");
    }

    /**
     * Construct table schema from info stored in SSTable's Stats.db, using the specified keyspace and table names.
     *
     * @param desc SSTable's descriptor
     * @return Restored CFMetaData
     * @throws IOException when Stats.db cannot be read
     */
    public static TableMetadata metadataFromSSTable(Descriptor desc, String keyspaceName, String tableName) throws IOException
    {
        if (!desc.version.isCompatible())
            throw new IOException("Unsupported SSTable version " + desc.getFormat().name() + "/" + desc.version);

        StatsComponent statsComponent = StatsComponent.load(desc, MetadataType.STATS, MetadataType.HEADER);
        SerializationHeader.Component header = statsComponent.serializationHeader();
        TableMetadata metadata = metadataFromHeader(desc, header, keyspaceName, tableName, false);

        // Cassandra 3.0 writes frozen user types into the serialization header without marking them as frozen, and so
        // do the "me" sstables written by 3.11.11 in the legacy test data. So in an sstable of a 3.x format, a user type
        // column that the header gives as non-frozen can be frozen. A node takes the type from its schema, but offline
        // the data has to tell: these columns are taken as frozen if the first rows that have data for them cannot be
        // read with the types of the header, but can with all of them frozen. They are decided together: if the sstable
        // also has a non-frozen user type column, the header's types are kept, unless the sstable reads with all of
        // them frozen anyway. That is the case of an sstable whose only row sets the non-frozen column to null, which
        // is then read as an empty frozen value.
        Set<ColumnIdentifier> userTypeColumns = maybeFrozenUserTypeColumns(desc, metadata);
        if (userTypeColumns.isEmpty() || readsColumns(desc, metadata, userTypeColumns))
            return metadata;

        TableMetadata frozen = metadataFromHeader(desc, header, keyspaceName, tableName, true);
        return readsColumns(desc, frozen, userTypeColumns) ? frozen : metadata;
    }

    private static TableMetadata metadataFromHeader(Descriptor desc, SerializationHeader.Component header,
                                                    String keyspaceName, String tableName, boolean freezeUserTypes) throws IOException
    {
        IPartitioner partitioner = FBUtilities.newPartitioner(desc);
        boolean isCounter = header.getRegularColumns().values().stream().anyMatch(AbstractType::isCounter) || header.getStaticColumns().values().stream().anyMatch(AbstractType::isCounter);
        boolean tuplesAreFrozen = hasOnlyFrozenPlainTuples(desc.version);

        TableMetadata.Builder builder = TableMetadata.builder(keyspaceName, tableName).partitioner(partitioner);
        header.getStaticColumns().entrySet().stream()
              .forEach(entry -> {
                  ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                  builder.addStaticColumn(ident, columnType(fixHeaderType(tuplesAreFrozen, entry.getKey(), entry.getValue(), false, isCounter), freezeUserTypes));
              });
        header.getRegularColumns().entrySet().stream()
              .forEach(entry -> {
                  ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                  builder.addRegularColumn(ident, columnType(fixHeaderType(tuplesAreFrozen, entry.getKey(), entry.getValue(), false, isCounter), freezeUserTypes));
              });
        builder.addPartitionKeyColumn("PartitionKey", fixHeaderType(tuplesAreFrozen, UTF8Type.instance.decompose("PartitionKey"), header.getKeyType(), true, isCounter));
        for (int i = 0; i < header.getClusteringTypes().size(); i++)
        {
            String name = "clustering" + (i > 0 ? i : "");
            builder.addClusteringColumn(name, fixHeaderType(tuplesAreFrozen, UTF8Type.instance.decompose(name), header.getClusteringTypes().get(i), true, isCounter));
        }
        if (SecondaryIndexManager.isIndexColumnFamily(desc.cfname))
        {
            String index = SecondaryIndexManager.getIndexName(desc.cfname);
            // Just set the Kind of index to CUSTOM, which is an irrelevant parameter that doesn't make any effect on the result
            IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata(index, IndexMetadata.Kind.CUSTOM, null);
            Indexes indexes = Indexes.of(indexMetadata);
            builder.indexes(indexes);
            builder.kind(TableMetadata.Kind.INDEX);
        }
        builder.isCounter(isCounter);
        return builder.build();
    }

    private static AbstractType<?> columnType(AbstractType<?> type, boolean freezeUserTypes)
    {
        return freezeUserTypes && type.isUDT() && type.isMultiCell() ? type.freeze() : type;
    }

    /**
     * Returns the columns of a non-frozen user type, if the sstable is of a format written by Cassandra 3.x.
     */
    private static Set<ColumnIdentifier> maybeFrozenUserTypeColumns(Descriptor desc, TableMetadata metadata)
    {
        Set<ColumnIdentifier> columns = new HashSet<>();
        if (!desc.version.format.name().equals(BigFormat.NAME) || desc.version.version.compareTo("na") >= 0)
            return columns;

        for (ColumnMetadata column : metadata.regularAndStaticColumns())
        {
            if (column.type.isUDT() && column.type.isMultiCell())
                columns.add(column.name);
        }
        return columns;
    }

    /**
     * How many rows with data for the columns in question {@link #readsColumns} reads at most. Reading a value with the
     * wrong type does not always fail on the row it is in, but usually does on one of the next unfiltereds.
     */
    private static final int TRIAL_ROWS_WITH_DATA = 100;

    /**
     * Whether the sstable can be read with the specified metadata up to {@link #TRIAL_ROWS_WITH_DATA} rows that have
     * data for one of the specified columns and the unfiltered that follows them, or up to its end.
     */
    private static boolean readsColumns(Descriptor desc, TableMetadata metadata, Set<ColumnIdentifier> columns)
    {
        SSTableReader sstable = null;
        try
        {
            sstable = SSTableReader.openNoValidation(null, desc, TableMetadataRef.forOfflineTools(metadata));
            int rowsWithData = 0;
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                // hasNext() reads the next partition or unfiltered, so it is called before the count is checked
                while (scanner.hasNext() && rowsWithData < TRIAL_ROWS_WITH_DATA)
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        if (hasDataFor(partition.staticRow(), columns))
                            rowsWithData++;

                        while (partition.hasNext() && rowsWithData < TRIAL_ROWS_WITH_DATA)
                        {
                            Unfiltered unfiltered = partition.next();
                            if (unfiltered.isRow() && hasDataFor((Row) unfiltered, columns))
                                rowsWithData++;
                        }
                    }
                }
            }
            return true;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            return false;
        }
        finally
        {
            if (sstable != null)
                sstable.selfRef().release();
        }
    }

    private static boolean hasDataFor(Row row, Set<ColumnIdentifier> columns)
    {
        for (ColumnData data : row)
        {
            if (columns.contains(data.column().name))
                return true;
        }
        return false;
    }

    /**
     * Whether every plain tuple ({@code TupleType}, not a UDT) in the headers of this sstable version is frozen. That
     * is so for the {@code big} versions before {@code nb}, which only Apache Cassandra writes: it has no multi-cell
     * tuples, and records frozen tuples without a {@code FrozenType} wrapper. A DataStax node also writes later
     * versions, in which a multi-cell plain tuple can be the type of a dropped non-frozen UDT column (see
     * {@link SerializationHeader.Component#toHeader}).
     */
    @VisibleForTesting
    static boolean hasOnlyFrozenPlainTuples(Version version)
    {
        return version.format.name().equals(BigFormat.NAME) && version.version.compareTo("nb") < 0;
    }

    /**
     * Fixes a column type read from an sstable header the way opening the sstable does
     * ({@link SerializationHeader.Component#toHeader}), as far as that can be done without the schema. A frozen tuple
     * recorded without a {@code FrozenType} wrapper, as Apache Cassandra records them, parses as a multi-cell tuple, so
     * its cells would be read as the cells of a complex column. Such a tuple is frozen if {@code tuplesAreFrozen}, or if
     * it has multi-cell subtypes, which a frozen tuple recorded that way has, and a dropped non-frozen UDT has not.
     * Otherwise a multi-cell plain tuple is kept, as it can be a dropped non-frozen UDT: with scalar fields only, the
     * two record the same type. Types that are valid, or that can't be fixed, are returned unchanged.
     */
    @VisibleForTesting
    static AbstractType<?> fixHeaderType(boolean tuplesAreFrozen, ByteBuffer name, AbstractType<?> type, boolean isPrimaryKeyColumn, boolean isCounter)
    {
        try
        {
            // isForOfflineTool accepts a multi-cell plain tuple whose subtypes are all frozen
            type.validateForColumn(name, isPrimaryKeyColumn, isCounter, false, !tuplesAreFrozen);
            return type;
        }
        catch (InvalidColumnTypeException e)
        {
            AbstractType<?> fixed = SerializationHeader.Component.tryFix(type, name, isPrimaryKeyColumn, isCounter, false, true);
            return fixed == null ? type : fixed;
        }
    }
}