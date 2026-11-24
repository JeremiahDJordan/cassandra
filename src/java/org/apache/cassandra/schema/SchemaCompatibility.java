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

package org.apache.cassandra.schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.functions.masking.ColumnMask;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractGeometricType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.DateRangeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Checks that schema changes are ones the nodes of an Apache Cassandra release read and accept, so that those nodes
 * can share the cluster and every node can go back to that release. Apache validates schema only when a statement
 * changes it, never when it loads it, and re-validates a table as a whole on every change, so every new or changed
 * element is checked whole.
 * <p>
 * Classes under {@code org.apache.cassandra.} must be ones the release has; other classes pass, since they load here
 * and come from the schema being upgraded. The checks read this node's settings where Apache reads its own: memtable
 * configurations, auto-repair, custom vector index options and data directories.
 */
public final class SchemaCompatibility
{
    public enum Target
    {
        APACHE_5_0("Apache Cassandra 5.0");

        private final String description;

        Target(String description)
        {
            this.description = description;
        }

        @Override
        public String toString()
        {
            return description;
        }
    }

    private static final String APACHE_PACKAGE = "org.apache.cassandra.";

    // SchemaConstants.NAME_LENGTH (cassandra-5.0.9)
    private static final int NAME_LENGTH = 48;

    // locator (cassandra-5.0.9)
    private static final Set<String> REPLICATION_CLASSES = ImmutableSet.of("org.apache.cassandra.locator.SimpleStrategy",
                                                                           "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                                                           "org.apache.cassandra.locator.LocalStrategy");

    // CompactionParams.Option and AbstractCompactionStrategy#validateOptions (cassandra-5.0.9)
    private static final Set<String> COMPACTION_OPTIONS = ImmutableSet.of("class",
                                                                          "enabled",
                                                                          "provide_overlapping_tombstones",
                                                                          "only_purge_repaired_tombstones",
                                                                          "tombstone_threshold",
                                                                          "tombstone_compaction_interval",
                                                                          "unchecked_tombstone_compaction",
                                                                          "log_all");

    // SizeTieredCompactionStrategyOptions (cassandra-5.0.9)
    private static final Set<String> SIZE_TIERED_OPTIONS = ImmutableSet.of("min_sstable_size", "bucket_low", "bucket_high");

    private static final Set<String> THRESHOLD_OPTIONS = ImmutableSet.of("min_threshold", "max_threshold");

    // the validateOptions methods of each strategy (cassandra-5.0.9 SizeTieredCompactionStrategy,
    // LeveledCompactionStrategy, TimeWindowCompactionStrategy and unified/Controller)
    private static final Map<String, Set<String>> COMPACTION_STRATEGY_OPTIONS =
        ImmutableMap.of("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
                        Sets.union(THRESHOLD_OPTIONS, SIZE_TIERED_OPTIONS),
                        "org.apache.cassandra.db.compaction.LeveledCompactionStrategy",
                        ImmutableSet.<String>builder().add("sstable_size_in_mb", "fanout_size", "single_sstable_uplevel")
                                                      .addAll(THRESHOLD_OPTIONS)
                                                      .addAll(SIZE_TIERED_OPTIONS)
                                                      .build(),
                        "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy",
                        ImmutableSet.<String>builder().add("timestamp_resolution",
                                                           "compaction_window_unit",
                                                           "compaction_window_size",
                                                           "expired_sstable_check_frequency_seconds",
                                                           "unsafe_aggressive_sstable_expiration")
                                                      .addAll(THRESHOLD_OPTIONS)
                                                      .addAll(SIZE_TIERED_OPTIONS)
                                                      .build(),
                        "org.apache.cassandra.db.compaction.UnifiedCompactionStrategy",
                        ImmutableSet.of("scaling_parameters",
                                        "min_sstable_size",
                                        "flush_size_override",
                                        "base_shard_count",
                                        "target_sstable_size",
                                        "sstable_growth",
                                        "max_sstables_to_compact",
                                        "unsafe_aggressive_sstable_expiration",
                                        "expired_sstable_check_frequency_seconds",
                                        "overlap_inclusion_method"));

    private static final String UNIFIED_COMPACTION_STRATEGY = "org.apache.cassandra.db.compaction.UnifiedCompactionStrategy";

    // unified/Controller and utils/Overlaps; the default target size is that of a node without
    // -Dunified_compaction.target_sstable_size (cassandra-5.0.9)
    private static final long UNIFIED_DEFAULT_TARGET_SSTABLE_SIZE = 1L << 30;
    private static final long UNIFIED_MIN_TARGET_SSTABLE_SIZE = 1L << 20;
    private static final Set<String> OVERLAP_INCLUSION_METHODS = ImmutableSet.of("NONE", "SINGLE", "TRANSITIVE");

    // UnifiedCompactionStrategy#parseScalingParameter (cassandra-5.0.9)
    private static final Pattern SCALING_PARAMETER = Pattern.compile("N|L\\d+|T\\d+|[+-]?\\d+");

    // CompressionParams (cassandra-5.0.9)
    private static final Set<String> COMPRESSION_OPTIONS = ImmutableSet.of("class", "chunk_length_in_kb", "min_compress_ratio", "enabled");

    // the supportedOptions methods of each compressor (cassandra-5.0.9 io/compress)
    private static final Map<String, Set<String>> COMPRESSOR_OPTIONS =
        ImmutableMap.of("org.apache.cassandra.io.compress.LZ4Compressor", ImmutableSet.of("lz4_compressor_type", "lz4_high_compressor_level"),
                        "org.apache.cassandra.io.compress.SnappyCompressor", ImmutableSet.of(),
                        "org.apache.cassandra.io.compress.DeflateCompressor", ImmutableSet.of(),
                        "org.apache.cassandra.io.compress.ZstdCompressor", ImmutableSet.of("compression_level"),
                        "org.apache.cassandra.io.compress.NoopCompressor", ImmutableSet.of());

    // db/memtable (cassandra-5.0.9)
    private static final Set<String> MEMTABLE_CLASSES = ImmutableSet.of("org.apache.cassandra.db.memtable.SkipListMemtable",
                                                                        "org.apache.cassandra.db.memtable.SkipListMemtableFactory",
                                                                        "org.apache.cassandra.db.memtable.ShardedSkipListMemtable",
                                                                        "org.apache.cassandra.db.memtable.TrieMemtable");

    // index implementations (cassandra-5.0.9)
    private static final Set<String> INDEX_CLASSES = ImmutableSet.of("org.apache.cassandra.index.sai.StorageAttachedIndex",
                                                                     "org.apache.cassandra.index.sasi.SASIIndex");

    // StorageAttachedIndex#VALID_OPTIONS and IndexWriterConfig (cassandra-5.0.9)
    private static final String MAXIMUM_NODE_CONNECTIONS = "maximum_node_connections";
    private static final String CONSTRUCTION_BEAM_WIDTH = "construction_beam_width";
    private static final String OPTIMIZE_FOR = "optimize_for";
    private static final Set<String> ANALYSIS_OPTIONS = ImmutableSet.of("case_sensitive", "normalize", "ascii");
    private static final Set<String> SAI_OPTIONS = ImmutableSet.<String>builder().add(IndexTarget.TARGET_OPTION_NAME,
                                                                                      IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                                                                                      MAXIMUM_NODE_CONNECTIONS,
                                                                                      CONSTRUCTION_BEAM_WIDTH,
                                                                                      "similarity_function",
                                                                                      OPTIMIZE_FOR)
                                                                                 .addAll(ANALYSIS_OPTIONS)
                                                                                 .build();
    // index/sai/disk/v1/vector/OptimizeFor (cassandra-5.0.9)
    private static final Set<String> OPTIMIZE_FOR_VALUES = ImmutableSet.of("LATENCY", "RECALL");

    private static final Set<String> NATIVE_FUNCTIONS_NOT_IN_APACHE = ImmutableSet.of("sai_analyze", "normalize_l2", "random_float_vector");

    private SchemaCompatibility()
    {
    }

    /**
     * The elements created or changed from {@code before} to {@code after} that {@code target} doesn't read or accept,
     * each described with the reason; empty if there are none.
     */
    public static List<String> incompatibilities(Target target, Keyspaces before, Keyspaces after)
    {
        List<String> incompatibilities = new ArrayList<>();
        KeyspacesDiff diff = Keyspaces.diff(before, after);

        for (KeyspaceMetadata keyspace : diff.created)
        {
            checkKeyspace(keyspace, incompatibilities);
            keyspace.types.forEach(type -> checkType(type, incompatibilities));
            keyspace.tables.forEach(table -> checkTable(table, incompatibilities));
            keyspace.views.forEach(view -> checkTable(view.metadata, incompatibilities));
            keyspace.userFunctions.udfs().forEach(function -> checkFunction(function, incompatibilities));
            keyspace.userFunctions.udas().forEach(aggregate -> checkAggregate(aggregate, incompatibilities));
        }

        for (KeyspaceDiff keyspaceDiff : diff.altered)
        {
            if (!keyspaceDiff.before.params.equals(keyspaceDiff.after.params))
                checkKeyspace(keyspaceDiff.after, incompatibilities);
            keyspaceDiff.types.created.forEach(type -> checkType(type, incompatibilities));
            keyspaceDiff.types.altered.forEach(altered -> checkType(altered.after, incompatibilities));
            keyspaceDiff.tables.created.forEach(table -> checkTable(table, incompatibilities));
            keyspaceDiff.tables.altered.forEach(altered -> checkTable(altered.after, incompatibilities));
            keyspaceDiff.views.created.forEach(view -> checkTable(view.metadata, incompatibilities));
            keyspaceDiff.views.altered.forEach(altered -> checkTable(altered.after.metadata, incompatibilities));
            keyspaceDiff.udfs.created.udfs().forEach(function -> checkFunction(function, incompatibilities));
            keyspaceDiff.udfs.altered.forEach(altered -> checkFunction(altered.after, incompatibilities));
            keyspaceDiff.udas.created.udas().forEach(aggregate -> checkAggregate(aggregate, incompatibilities));
            keyspaceDiff.udas.altered.forEach(altered -> checkAggregate(altered.after, incompatibilities));
        }

        return incompatibilities;
    }

    private static void checkKeyspace(KeyspaceMetadata keyspace, List<String> incompatibilities)
    {
        String element = "keyspace " + keyspace.name;
        if (keyspace.name.length() > NAME_LENGTH)
            incompatibilities.add(String.format("%s: name longer than %d characters", element, NAME_LENGTH));
        checkClass(element, "replication strategy", keyspace.params.replication.klass.getName(), REPLICATION_CLASSES, incompatibilities);
    }

    private static void checkType(UserType type, List<String> incompatibilities)
    {
        String element = "type " + type.keyspace + '.' + type.getNameAsString();
        type.fieldTypes().forEach(fieldType -> checkDataType(element, fieldType, incompatibilities));
    }

    private static void checkTable(TableMetadata table, List<String> incompatibilities)
    {
        String element = (table.isView() ? "view " : "table ") + table.keyspace + '.' + table.name;

        for (ColumnMetadata column : table.columns())
        {
            checkDataType(element, column.type, incompatibilities);
            ColumnMask mask = column.getMask();
            if (mask != null)
                checkNativeFunction(element, mask.function, incompatibilities);
        }

        // a dropped non-frozen user type column is recorded with a non-frozen tuple type, which Apache doesn't have
        for (DroppedColumn dropped : table.droppedColumns.values())
        {
            checkDataType(element, dropped.column.type, incompatibilities);
            if (dropped.column.type instanceof TupleType && dropped.column.type.isMultiCell())
                incompatibilities.add(String.format("%s: dropped non-frozen column %s of a user type", element, dropped.column.name));
        }

        checkCompaction(element, table.params.compaction, incompatibilities);
        checkCompression(element, table.params.compression, incompatibilities);

        if (table.params.memtable != MemtableParams.DEFAULT)
        {
            String key = table.params.memtable.configurationKey();
            String memtableClass = MemtableParams.configurationClassName(key);
            if (memtableClass == null)
                incompatibilities.add(String.format("%s: memtable configuration '%s' isn't defined in this node's memtable configurations", element, key));
            else
                checkClass(element, "memtable configuration '" + key + "' of", memtableClass, MEMTABLE_CLASSES, incompatibilities);
        }

        if (!table.params.autoRepair.equals(AutoRepairParams.DEFAULT) && !CassandraRelevantProperties.AUTOREPAIR_ENABLE.getBoolean())
            incompatibilities.add(String.format("%s: auto_repair needs %s", element, CassandraRelevantProperties.AUTOREPAIR_ENABLE.getKey()));

        Set<Pair<ColumnMetadata, IndexTarget.Type>> storageAttachedTargets = new HashSet<>();
        for (IndexMetadata index : table.indexes)
        {
            checkIndex(element, table, index, incompatibilities);

            // cassandra-5.0.9 StorageAttachedIndex#validateOptions allows one storage-attached index per target
            Pair<ColumnMetadata, IndexTarget.Type> target = target(table, index);
            if (target != null && StorageAttachedIndex.class.getName().equals(index.getIndexClassName()) && !storageAttachedTargets.add(target))
                incompatibilities.add(String.format("%s: more than one storage-attached index on column %s", element, target.left.name));
        }

        for (TriggerMetadata trigger : table.triggers)
            checkClass(element, "trigger", trigger.classOption, ImmutableSet.of(), incompatibilities);
    }

    private static void checkCompaction(String element, CompactionParams compaction, List<String> incompatibilities)
    {
        Map<String, String> options = compaction.asMap();
        String strategy = compaction.klass().getName();
        if (!checkClass(element, "compaction strategy", strategy, COMPACTION_STRATEGY_OPTIONS.keySet(), incompatibilities)
            || !strategy.startsWith(APACHE_PACKAGE))
            return;

        Set<String> strategyOptions = COMPACTION_STRATEGY_OPTIONS.get(strategy);
        for (String option : options.keySet())
        {
            if (!COMPACTION_OPTIONS.contains(option) && !strategyOptions.contains(option))
                incompatibilities.add(String.format("%s: compaction option '%s' of %s", element, option, compaction.klass().getSimpleName()));
        }

        String scalingParameters = options.get("scaling_parameters");
        if (scalingParameters != null)
        {
            for (String parameter : scalingParameters.split(","))
            {
                if (!SCALING_PARAMETER.matcher(parameter.trim()).matches())
                    incompatibilities.add(String.format("%s: compaction scaling parameter '%s'", element, parameter.trim()));
            }
        }

        if (strategy.equals(UNIFIED_COMPACTION_STRATEGY))
        {
            try
            {
                validateUnifiedCompactionOptions(options);
            }
            catch (ConfigurationException | NumberFormatException e)
            {
                incompatibilities.add(String.format("%s: compaction options of UnifiedCompactionStrategy: %s", element, e.getMessage()));
            }
        }
    }

    /**
     * Throws for the UnifiedCompactionStrategy option values Apache rejects, as cassandra-5.0.9
     * unified/Controller#validateOptions does; the scaling parameters are checked separately.
     */
    private static void validateUnifiedCompactionOptions(Map<String, String> options)
    {
        String s = options.get("base_shard_count");
        if (s != null && Integer.parseInt(s) <= 0)
            throw new ConfigurationException("base_shard_count should be positive: " + s);

        long targetSSTableSize = UNIFIED_DEFAULT_TARGET_SSTABLE_SIZE;
        s = options.get("target_sstable_size");
        if (s != null)
        {
            double targetSize = FBUtilities.parseHumanReadable(s, null, "B");
            if (targetSize >= Long.MAX_VALUE || targetSize < UNIFIED_MIN_TARGET_SSTABLE_SIZE)
                throw new ConfigurationException("target_sstable_size " + s + " is out of range");
            targetSSTableSize = (long) Math.ceil(targetSize);
        }

        s = options.get("flush_size_override");
        if (s != null && FBUtilities.parseHumanReadableBytes(s) < UNIFIED_MIN_TARGET_SSTABLE_SIZE)
            throw new ConfigurationException("flush_size_override " + s + " is below 1MiB");

        s = options.get("max_sstables_to_compact");
        if (s != null)
            Integer.parseInt(s);

        s = options.get("expired_sstable_check_frequency_seconds");
        if (s != null && Long.parseLong(s) <= 0)
            throw new ConfigurationException("expired_sstable_check_frequency_seconds should be positive: " + s);

        s = options.get("unsafe_aggressive_sstable_expiration");
        if (s != null && !s.equalsIgnoreCase("true") && !s.equalsIgnoreCase("false"))
            throw new ConfigurationException("unsafe_aggressive_sstable_expiration should be true or false: " + s);

        s = options.get("overlap_inclusion_method");
        if (s != null && !OVERLAP_INCLUSION_METHODS.contains(s.toUpperCase()))
            throw new ConfigurationException("overlap_inclusion_method " + s);

        s = options.get("min_sstable_size");
        if (s != null)
        {
            long minSSTableSize = FBUtilities.parseHumanReadableBytes(s);
            if (minSSTableSize < 0 || minSSTableSize >= (long) Math.ceil(targetSSTableSize * Math.sqrt(0.5)))
                throw new ConfigurationException("min_sstable_size " + s + " should be at least 0 and less than 70% of the target SSTable size");
        }

        s = options.get("sstable_growth");
        if (s != null)
        {
            double growth = FBUtilities.parsePercent(s);
            if (growth < 0 || growth > 1)
                throw new ConfigurationException("sstable_growth " + s + " should be between 0 and 1");
        }
    }

    private static void checkCompression(String element, CompressionParams compression, List<String> incompatibilities)
    {
        if (!compression.isEnabled())
            return;

        String compressor = compression.klass().getName();
        if (!checkClass(element, "compressor", compressor, COMPRESSOR_OPTIONS.keySet(), incompatibilities)
            || !compressor.startsWith(APACHE_PACKAGE))
            return;

        Set<String> compressorOptions = COMPRESSOR_OPTIONS.get(compressor);
        for (String option : compression.asMap().keySet())
        {
            if (!COMPRESSION_OPTIONS.contains(option) && !compressorOptions.contains(option))
                incompatibilities.add(String.format("%s: compression option '%s' of %s", element, option, compression.klass().getSimpleName()));
        }
    }

    private static void checkIndex(String tableElement, TableMetadata table, IndexMetadata index, List<String> incompatibilities)
    {
        String element = tableElement + " index " + index.name;
        if (!index.isCustom())
            return;

        String indexClass = index.getIndexClassName();
        checkClass(element, "index", indexClass, INDEX_CLASSES, incompatibilities);

        Pair<ColumnMetadata, IndexTarget.Type> target = target(table, index);
        if (target == null)
            return;

        // cassandra-5.0.9 CreateIndexStatement#validateCustomIndexColumnName
        String targetName = target.left.name.toString();
        if (!SchemaConstants.isValidCharsName(targetName) || targetName.length() > NAME_LENGTH)
            incompatibilities.add(String.format("%s: target column name '%s' isn't a word of at most %d characters", element, targetName, NAME_LENGTH));

        if (!StorageAttachedIndex.class.getName().equals(indexClass))
            return;

        for (String option : index.options.keySet())
        {
            if (!SAI_OPTIONS.contains(option))
                incompatibilities.add(String.format("%s: option '%s'", element, option));
        }

        if (!CassandraRelevantProperties.SAI_VECTOR_ALLOW_CUSTOM_PARAMETERS.getBoolean())
        {
            for (String option : new String[]{ MAXIMUM_NODE_CONNECTIONS, CONSTRUCTION_BEAM_WIDTH })
            {
                if (index.options.containsKey(option))
                    incompatibilities.add(String.format("%s: option '%s' needs %s", element, option, CassandraRelevantProperties.SAI_VECTOR_ALLOW_CUSTOM_PARAMETERS.getKey()));
            }
        }

        String optimizeFor = index.options.get(OPTIMIZE_FOR);
        if (optimizeFor != null && !OPTIMIZE_FOR_VALUES.contains(optimizeFor.toUpperCase()))
            incompatibilities.add(String.format("%s: %s '%s'", element, OPTIMIZE_FOR, optimizeFor));

        AbstractType<?> indexedType = TypeUtil.cellValueType(target.left, target.right).unwrap();
        boolean analyzed = index.options.keySet().stream().anyMatch(ANALYSIS_OPTIONS::contains);
        if (analyzed && (target.left.isPrimaryKeyColumn() || !(indexedType instanceof UTF8Type || indexedType instanceof AsciiType)))
            incompatibilities.add(String.format("%s: analysis options on column %s", element, target.left.name));

        if (StorageAttachedIndex.indexesBlobTerms(indexedType))
            incompatibilities.add(String.format("%s: index on blob values of column %s", element, target.left.name));

        if (indexedType instanceof VectorType)
        {
            if (!(((VectorType<?>) indexedType).elementType instanceof FloatType))
                incompatibilities.add(String.format("%s: vector index on non-float vector column %s", element, target.left.name));
            if (DatabaseDescriptor.getNonLocalSystemKeyspacesDataFileLocations().length > 1)
                incompatibilities.add(String.format("%s: vector index with more than one data directory", element));
        }
    }

    /**
     * The column and target type of an index with a target, or null for one without.
     */
    private static Pair<ColumnMetadata, IndexTarget.Type> target(TableMetadata table, IndexMetadata index)
    {
        String target = index.options.get(IndexTarget.TARGET_OPTION_NAME);
        return target == null ? null : TargetParser.parse(table, target);
    }

    private static void checkFunction(UDFunction function, List<String> incompatibilities)
    {
        String element = "function " + function.name();
        if (function.isDeterministic() || function.isMonotonic() || !function.monotonicOn().isEmpty())
            incompatibilities.add(String.format("%s: DETERMINISTIC or MONOTONIC", element));
        function.argTypes().forEach(type -> checkDataType(element, type, incompatibilities));
        checkDataType(element, function.returnType(), incompatibilities);
    }

    private static void checkAggregate(UDAggregate aggregate, List<String> incompatibilities)
    {
        String element = "aggregate " + aggregate.name();
        if (aggregate.isDeterministic())
            incompatibilities.add(String.format("%s: DETERMINISTIC", element));
        checkNativeFunction(element, aggregate.stateFunction(), incompatibilities);
        if (aggregate.finalFunction() != null)
            checkNativeFunction(element, aggregate.finalFunction(), incompatibilities);
        aggregate.argTypes().forEach(type -> checkDataType(element, type, incompatibilities));
        checkDataType(element, aggregate.stateType(), incompatibilities);
        checkDataType(element, aggregate.returnType(), incompatibilities);
    }

    private static void checkNativeFunction(String element, Function function, List<String> incompatibilities)
    {
        FunctionName name = function.name();
        if (SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(name.keyspace) && NATIVE_FUNCTIONS_NOT_IN_APACHE.contains(name.name))
            incompatibilities.add(String.format("%s: function %s", element, name));
    }

    private static void checkDataType(String element, AbstractType<?> type, List<String> incompatibilities)
    {
        AbstractType<?> unwrapped = type.unwrap();
        if (unwrapped instanceof AbstractGeometricType || unwrapped instanceof DateRangeType)
        {
            incompatibilities.add(String.format("%s: type %s", element, unwrapped.asCQL3Type()));
            return;
        }
        for (AbstractType<?> subType : unwrapped.subTypes())
            checkDataType(element, subType, incompatibilities);
    }

    /**
     * Adds an incompatibility, and returns false, when {@code className} is under {@code org.apache.cassandra.} and
     * isn't one of {@code apacheClasses}.
     */
    private static boolean checkClass(String element, String kind, String className, Set<String> apacheClasses, List<String> incompatibilities)
    {
        if (!className.startsWith(APACHE_PACKAGE) || apacheClasses.contains(className))
            return true;

        incompatibilities.add(String.format("%s: %s %s", element, kind, className));
        return false;
    }
}
