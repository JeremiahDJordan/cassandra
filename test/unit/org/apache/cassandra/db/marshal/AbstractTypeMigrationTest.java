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

package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import net.openhft.chronicle.core.util.ThrowingFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractTypeTest.CurrentTypesCompatibility;
import org.apache.cassandra.db.marshal.AbstractTypeTest.TypesCompatibility;
import org.apache.cassandra.exceptions.InvalidColumnTypeException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.asserts.SoftAssertionsWithLimit;
import org.assertj.core.api.SoftAssertions;

import static org.apache.cassandra.db.marshal.AbstractTypeTest.compatibilityFile;
import static org.apache.cassandra.db.marshal.AbstractTypeTest.configureTypeGenerators;
import static org.apache.cassandra.db.marshal.AbstractTypeTest.isCompatibleWithDesc;
import static org.apache.cassandra.db.marshal.AbstractTypeTest.isSerializationCompatibleWithDesc;
import static org.apache.cassandra.db.marshal.AbstractTypeTest.isValueCompatibleWithDesc;
import static org.apache.cassandra.utils.AbstractTypeGenerators.forEachTypesPair;

/**
 * Compares the type relations of this version against the compatibility files of older versions, in both directions
 * where a rollback is supported. Split from {@link AbstractTypeTest} so that these comparisons, each a walk over every type
 * pair, have their own test timeout; type relations do not depend on {@link AbstractTypeTest}'s row implementation
 * parameter, so this class is not parameterised.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class AbstractTypeMigrationTest
{
    /**
     * The three relations between types that the compatibility files record. For each, {@code left} is the new type and
     * {@code right} the previous one.
     */
    enum TypeRelation
    {
        COMPATIBLE(AbstractType::isCompatibleWith),
        SERIALIZATION_COMPATIBLE(AbstractType::isSerializationCompatibleWith),
        VALUE_COMPATIBLE(AbstractType::isValueCompatibleWith);

        private final BiPredicate<AbstractType<?>, AbstractType<?>> relation;

        TypeRelation(BiPredicate<AbstractType<?>, AbstractType<?>> relation)
        {
            this.relation = relation;
        }

        boolean test(AbstractType<?> left, AbstractType<?> right)
        {
            return relation.test(left, right);
        }

        boolean expected(TypesCompatibility version, AbstractType<?> left, AbstractType<?> right)
        {
            switch (this)
            {
                case COMPATIBLE:
                    return version.expectCompatibleWith(left, right);
                case SERIALIZATION_COMPATIBLE:
                    return version.expectSerializationCompatibleWith(left, right);
                case VALUE_COMPATIBLE:
                    return version.expectValueCompatibleWith(left, right);
                default:
                    throw new AssertionError(this);
            }
        }
    }

    /**
     * A documented place where one version answers {@code false} for a relation that the other answered {@code true}
     * for: an upgrade difference, where the current version is the stricter one, or a downgrade difference, where it is
     * more permissive than a version a rollback must still be able to use. These relations decide whether new DDL and CQL
     * is accepted; on stored data, {@code isValueCompatibleWith} picks between two versions of a column when rows are
     * merged ({@link org.apache.cassandra.db.rows.ColumnMetadataVersionComparator}).
     * <p>
     * Each constant matches only top-level pairs for which its rule is the sole reason for the different answer, so that
     * anything else, including a nested variant of a rule, still fails the test.
     */
    enum UpgradeDifference
    {
        /**
         * A frozen tuple or UDT against a non-frozen UDT: this version requires the same frozen-ness
         * ({@link MultiCellCapableType#isCompatibleWith}), as a frozen value is a single cell and a non-frozen UDT has one
         * cell per field. The frozen-ness of a column cannot be changed by DDL, so no stored schema depends on this.
         */
        FROZEN_AND_NON_FROZEN_UDTS_ARE_DISTINCT(TypeRelation.COMPATIBLE, TypeRelation.VALUE_COMPATIBLE)
        {
            @Override
            boolean matches(TypeRelation relation, AbstractType<?> left, AbstractType<?> right, TypesCompatibility stricterVersion)
            {
                boolean frozenAgainstNonFrozen = isNonFrozenUDT(left) && isFrozenTupleOrUDT(right)
                                                 || isFrozenTupleOrUDT(left) && isNonFrozenUDT(right);
                // the pair would be related if both sides had the same frozen-ness
                return frozenAgainstNonFrozen && relation.test(left.freeze(), right.freeze());
            }
        },

        /**
         * Non-frozen collections, tuples and UDTs: the part stored in the cell value (a map's values, a list's elements,
         * the fields of a tuple or UDT) must be serialization-compatible, not only value-compatible, because a fixed-length
         * and a variable-length type are framed differently in a cell ({@code CollectionType.isCompatibleWithMultiCell}).
         */
        MULTI_CELL_VALUES_MUST_BE_SERIALIZATION_COMPATIBLE(TypeRelation.COMPATIBLE, TypeRelation.VALUE_COMPATIBLE)
        {
            @Override
            boolean matches(TypeRelation relation, AbstractType<?> left, AbstractType<?> right, TypesCompatibility stricterVersion)
            {
                if (!left.isMultiCell() || !right.isMultiCell() || left.getClass() != right.getClass())
                    return false;

                if (left instanceof CollectionType)
                {
                    CollectionType<?> l = (CollectionType<?>) left;
                    CollectionType<?> r = (CollectionType<?>) right;
                    return l.nameComparator().isCompatibleWith(r.nameComparator())
                           && isValueButNotSerializationCompatible(ImmutableList.of(l.valueComparator()), ImmutableList.of(r.valueComparator()));
                }

                return left instanceof TupleType && isValueButNotSerializationCompatible(left.subTypes(), right.subTypes());
            }
        },

        /**
         * Frozen maps and sets: the keys (the elements of a set) must be compatible including for sorting, not only
         * value-compatible, because a frozen map or set is stored sorted by its keys
         * ({@code CollectionType.isValueCompatibleWithFrozen}).
         */
        FROZEN_MAP_KEYS_AND_SET_ELEMENTS_MUST_BE_COMPATIBLE(TypeRelation.VALUE_COMPATIBLE)
        {
            @Override
            boolean matches(TypeRelation relation, AbstractType<?> left, AbstractType<?> right, TypesCompatibility stricterVersion)
            {
                if (left.isMultiCell() || right.isMultiCell() || left.getClass() != right.getClass())
                    return false;

                if (!(left instanceof MapType) && !(left instanceof SetType))
                    return false;

                CollectionType<?> l = (CollectionType<?>) left;
                CollectionType<?> r = (CollectionType<?>) right;
                return !l.nameComparator().isCompatibleWith(r.nameComparator())
                       && l.nameComparator().isValueCompatibleWith(r.nameComparator())
                       && l.valueComparator().isValueCompatibleWith(r.valueComparator());
            }
        },

        /**
         * A downgrade difference: a UDT against a plain tuple with the same frozen-ness, which this version relates as the
         * tuple of the UDT's field types, while the DataStax 4.0 line requires the same class. Re-adding a dropped column
         * with the other type persists such a pair; a frozen tuple and a frozen UDT with the same fields have the same
         * bytes, and the DataStax 4.0 line reads a live column with the schema's type, so a rollback still reads it.
         */
        UDTS_AND_TUPLES_ARE_INTERCHANGEABLE(TypeRelation.COMPATIBLE, TypeRelation.VALUE_COMPATIBLE)
        {
            @Override
            boolean matches(TypeRelation relation, AbstractType<?> left, AbstractType<?> right, TypesCompatibility stricterVersion)
            {
                if (!(left instanceof TupleType) || !(right instanceof TupleType)
                    || left.isUDT() == right.isUDT() || left.isMultiCell() != right.isMultiCell())
                    return false;

                // the stricter version relates the pair once the UDT is written as the tuple of its field types; the
                // compatibility files do not record that a type is related to itself
                AbstractType<?> leftTuple = asPlainTuple(left);
                AbstractType<?> rightTuple = asPlainTuple(right);
                return leftTuple.equals(rightTuple) || relation.expected(stricterVersion, leftTuple, rightTuple);
            }
        };

        final Set<TypeRelation> relations;

        UpgradeDifference(TypeRelation first, TypeRelation... rest)
        {
            this.relations = EnumSet.of(first, rest);
        }

        /**
         * Whether {@code stricterVersion} answers {@code false} for {@code relation} between these types because of this
         * rule alone. For an upgrade difference, the stricter version is the current one, whose relations are the
         * {@link AbstractType} methods themselves.
         */
        abstract boolean matches(TypeRelation relation, AbstractType<?> left, AbstractType<?> right, TypesCompatibility stricterVersion);

        private static AbstractType<?> asPlainTuple(AbstractType<?> type)
        {
            return type.isUDT() ? new TupleType(type.subTypes(), type.isMultiCell()) : type;
        }

        private static boolean isNonFrozenUDT(AbstractType<?> type)
        {
            return type.isUDT() && type.isMultiCell();
        }

        private static boolean isFrozenTupleOrUDT(AbstractType<?> type)
        {
            return (type.isTuple() || type.isUDT()) && !type.isMultiCell();
        }

        /**
         * Whether every element of {@code previous} is value-compatible with the matching element of {@code current}, and at
         * least one of them is not serialization-compatible.
         */
        private static boolean isValueButNotSerializationCompatible(List<AbstractType<?>> current, List<AbstractType<?>> previous)
        {
            if (current.size() < previous.size())
                return false;

            boolean notSerializationCompatible = false;
            for (int i = 0; i < previous.size(); i++)
            {
                if (!current.get(i).isValueCompatibleWith(previous.get(i)))
                    return false;
                notSerializationCompatible |= !current.get(i).isSerializationCompatibleWith(previous.get(i));
            }
            return notSerializationCompatible;
        }
    }

    private final static class LoadedTypesCompatibility extends TypesCompatibility
    {
        private final Multimap<AbstractType<?>, AbstractType<?>> valueCompatibleWith;
        private final Multimap<AbstractType<?>, AbstractType<?>> serializationCompatibleWith;
        private final Multimap<AbstractType<?>, AbstractType<?>> compatibleWith;
        private final Set<Class<? extends AbstractType>> unsupportedTypes;
        private final Set<Class<? extends AbstractType>> knownTypes;
        private final Set<Class<? extends AbstractType>> multiCellSupportingTypes;
        private final Set<Class<? extends AbstractType>> multiCellSupportingTypesForReading;
        private final Set<AbstractType<?>> primitiveTypes;
        private final SoftAssertions loadAssertions = new SoftAssertionsWithLimit(100);
        private final Set<String> excludedTypes;
        private final UnaryOperator<AbstractType<?>> typeNormalizer;

        private <T> Function<Object, Stream<T>> safeParse(ThrowingFunction<String, T, Exception> consumer)
        {
            return obj -> {
                String typeName = (String) obj;
                if (refersExcludedType(typeName))
                    return Stream.empty();
                try
        {
                    return Stream.of(consumer.apply(typeName));
                }
                catch (InterruptedException ex)
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ex);
                }
                catch (Exception th)
                {
                    loadAssertions.fail("Failed to parse type: " + typeName, th);
                }
                return Stream.empty();
            };
        }

        private boolean refersExcludedType(String typeName)
        {
            for (String unsupportedType : excludedTypes)
                if (typeName.contains(unsupportedType))
                    return true;
            return false;
        }

        private Set<Class<? extends AbstractType>> getTypesArray(Object json)
        {
            return ((JSONArray) json).stream()
                                     .map(String.class::cast)
                                     .flatMap(safeParse(((ThrowingFunction<String, Class<? extends AbstractType>, Exception>) className -> (Class<? extends AbstractType>) Class.forName(className))))
                                     .collect(Collectors.toUnmodifiableSet());
        }

        private Multimap<AbstractType<?>, AbstractType<?>> getTypesCompatibilityMultimap(Object json)
        {
            Multimap<AbstractType<?>, AbstractType<?>> map = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
            ((JSONObject) json).forEach((l, collection) -> safeParse(this::parseType).apply(l).forEach(left -> {
                ((JSONArray) collection).forEach(r -> {
                    safeParse(this::parseType).apply(r).forEach(right -> map.put(left, right));
                });
            }));
            return map;
        }

        private AbstractType<?> parseType(String typeName)
        {
            return typeNormalizer.apply(TypeParser.parse(typeName));
        }

        private LoadedTypesCompatibility(Path path, Set<String> excludedTypes) throws IOException
        {
            this(path, excludedTypes, UnaryOperator.identity());
        }

        /**
         * @param typeNormalizer applied to every type parsed from the file, to read a file written by a version whose type
         *                       strings mean something different here; see {@link #freezeImplicitlyFrozenTuples}.
         */
        private LoadedTypesCompatibility(Path path, Set<String> excludedTypes, UnaryOperator<AbstractType<?>> typeNormalizer) throws IOException
        {
            super(path.getFileName().toString());

            this.excludedTypes = ImmutableSet.copyOf(excludedTypes);
            this.typeNormalizer = typeNormalizer;
            logger.info("Loading types compatibility from {} skipping {} as unsupported", path.toAbsolutePath(), excludedTypes);
            try (GZIPInputStream in = new GZIPInputStream(Files.newInputStream(path)))
            {
                JSONObject json = (JSONObject) new JSONParser().parse(new InputStreamReader(in, Charsets.UTF_8));
                knownTypes = getTypesArray(json.get(KNOWN_TYPES_KEY));
                multiCellSupportingTypes = getTypesArray(json.get(MULTICELL_TYPES_KEY));
                multiCellSupportingTypesForReading = getTypesArray(json.get(MULTICELL_TYPES_FOR_READING_KEY));
                unsupportedTypes = getTypesArray(json.get(UNSUPPORTED_TYPES_KEY));
                primitiveTypes = ((JSONArray) json.get(PRIMITIVE_TYPES_KEY)).stream().flatMap(safeParse(this::parseType)).collect(Collectors.toSet());
                compatibleWith = getTypesCompatibilityMultimap(json.get(COMPATIBLE_TYPES_KEY));
                serializationCompatibleWith = getTypesCompatibilityMultimap(json.get(SERIALIZATION_COMPATIBLE_TYPES_KEY));
                valueCompatibleWith = getTypesCompatibilityMultimap(json.get(VALUE_COMPATIBLE_TYPES_KEY));
            }
            catch (ParseException | NoSuchFileException e)
            {
                throw new IOException(path.toAbsolutePath().toString(), e);
            }

            logger.info("Loaded types compatibility from {}: knownTypes: {}, multiCellSupportingTypes: {}, " +
                        "multiCellSupportingTypesForReading: {}, unsupportedTypes: {}, primitiveTypes: {}, " +
                        "compatibleWith: {}, serializationCompatibleWith: {}, valueCompatibleWith: {}",
                        path.getFileName(), knownTypes.size(), multiCellSupportingTypes.size(),
                        multiCellSupportingTypesForReading.size(), unsupportedTypes.size(), primitiveTypes.size(),
                        compatibleWith.size(), serializationCompatibleWith.size(), valueCompatibleWith.size());
        }

        public void assertLoaded()
        {
            loadAssertions.assertAll();
        }
        @Override
        public boolean expectCompatibleWith(AbstractType left, AbstractType right)
        {
            return compatibleWith.containsEntry(left, right);
        }

        @Override
        public boolean expectValueCompatibleWith(AbstractType left, AbstractType right)
        {
            return valueCompatibleWith.containsEntry(left, right);
        }

        @Override
        public boolean expectSerializationCompatibleWith(AbstractType left, AbstractType right)
        {
            return serializationCompatibleWith.containsEntry(left, right);
        }

        @Override
        public Set<Class<? extends AbstractType>> knownTypes()
        {
            return knownTypes;
        }

        @Override
        public Set<AbstractType<?>> primitiveTypes()
        {
            return primitiveTypes;
        }

        @Override
        public Set<Class<? extends AbstractType>> multiCellSupportingTypes()
        {
            return multiCellSupportingTypes;
        }

        @Override
        public Set<Class<? extends AbstractType>> multiCellSupportingTypesForReading()
        {
            return multiCellSupportingTypesForReading;
        }

        @Override
        public Set<Class<? extends AbstractType>> unsupportedTypes()
        {
            return unsupportedTypes;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(AbstractTypeMigrationTest.class);

    static
    {
        configureTypeGenerators();
    }

    private static TypesCompatibility currentTypesCompatibility;
    private static LoadedTypesCompatibility cassandra40TypesCompatibility;
    private static LoadedTypesCompatibility cassandra41TypesCompatibility;
    private static LoadedTypesCompatibility cassandra50TypesCompatibility;
    private static LoadedTypesCompatibility dse68cndbTypesCompatibility;
    private static LoadedTypesCompatibility legacyCC40TypesCompatibility;
    private static LoadedTypesCompatibility cc40TypesCompatibility;
    private static LoadedTypesCompatibility cc50TypesCompatibility;

    /**
     * Where the current version is deliberately stricter than Apache Cassandra 4.0, 4.1 and 5.0. The DataStax 4.0 line and
     * DSE 6.8 have the same rule.
     */
    private static final Set<UpgradeDifference> APACHE_UPGRADE_DIFFERENCES = Sets.immutableEnumSet(UpgradeDifference.FROZEN_AND_NON_FROZEN_UDTS_ARE_DISTINCT);

    /**
     * Where the current version is deliberately stricter than the DataStax 4.0 line (the cc-4.0 and legacy-cc-4.0 files)
     * and DSE 6.8 as used by CNDB (the dse-6.8-cndb file). Apache 5.0 has both rules.
     */
    private static final Set<UpgradeDifference> DATASTAX_4_0_UPGRADE_DIFFERENCES = Sets.immutableEnumSet(UpgradeDifference.MULTI_CELL_VALUES_MUST_BE_SERIALIZATION_COMPATIBLE,
                                                                                                   UpgradeDifference.FROZEN_MAP_KEYS_AND_SET_ELEMENTS_MUST_BE_COMPATIBLE);

    /**
     * Where the current version is deliberately more permissive than the DataStax 4.0 line, which HCD 1.x is based on.
     */
    private static final Set<UpgradeDifference> HCD_1_DOWNGRADE_DIFFERENCES = Sets.immutableEnumSet(UpgradeDifference.UDTS_AND_TUPLES_ARE_INTERCHANGEABLE);

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        cassandra40TypesCompatibility = new LoadedTypesCompatibility(compatibilityFile(CassandraVersion.CASSANDRA_4_0.toMajorMinorString()), Set.of(), AbstractTypeMigrationTest::freezeImplicitlyFrozenTuples);
        cassandra41TypesCompatibility = new LoadedTypesCompatibility(compatibilityFile(CassandraVersion.CASSANDRA_4_1.toMajorMinorString()), Set.of(), AbstractTypeMigrationTest::freezeImplicitlyFrozenTuples);
        cassandra50TypesCompatibility = new LoadedTypesCompatibility(compatibilityFile(CassandraVersion.CASSANDRA_5_0.toMajorMinorString()), Set.of(), AbstractTypeMigrationTest::freezeImplicitlyFrozenTuples);
        dse68cndbTypesCompatibility = new LoadedTypesCompatibility(compatibilityFile("dse-6.8-cndb"), ImmutableSet.of());
        legacyCC40TypesCompatibility = new LoadedTypesCompatibility(compatibilityFile("legacy-cc-4.0"), ImmutableSet.of());
        cc40TypesCompatibility = new LoadedTypesCompatibility(compatibilityFile("cc-4.0"), ImmutableSet.of());
        cc50TypesCompatibility = new LoadedTypesCompatibility(compatibilityFile("cc-5.0"), ImmutableSet.of());
        currentTypesCompatibility = new CurrentTypesCompatibility();
    }

    @Test
    public void testMigrationFromCassandra40ToCurrent()
    {
        cassandra40TypesCompatibility.assertLoaded();
        testBackwardCompatibility(currentTypesCompatibility, cassandra40TypesCompatibility, true, APACHE_UPGRADE_DIFFERENCES);
    }

    @Test
    public void testMigrationFromCassandra41ToCurrent()
    {
        cassandra41TypesCompatibility.assertLoaded();
        testBackwardCompatibility(currentTypesCompatibility, cassandra41TypesCompatibility, true, APACHE_UPGRADE_DIFFERENCES);
    }

    @Test
    public void testMigrationFromCassandra50ToCurrent()
    {
        cassandra50TypesCompatibility.assertLoaded();
        testBackwardCompatibility(currentTypesCompatibility, cassandra50TypesCompatibility, true, APACHE_UPGRADE_DIFFERENCES);
    }

    @Test
    public void testMigrationFromDse68CndbToCurrent()
    {
        dse68cndbTypesCompatibility.assertLoaded();
        testBackwardCompatibility(currentTypesCompatibility, dse68cndbTypesCompatibility, false, DATASTAX_4_0_UPGRADE_DIFFERENCES);
    }

    @Test
    public void testMigrationFromLegacyCC40ToCurrent()
    {
        legacyCC40TypesCompatibility.assertLoaded();
        testBackwardCompatibility(currentTypesCompatibility, legacyCC40TypesCompatibility, false, DATASTAX_4_0_UPGRADE_DIFFERENCES);
    }

    @Test
    public void testMigrationFromCC40ToCurrent()
    {
        cc40TypesCompatibility.assertLoaded();
        testBackwardCompatibility(currentTypesCompatibility, cc40TypesCompatibility, false, DATASTAX_4_0_UPGRADE_DIFFERENCES);
    }

    @Test
    public void testMigrationFromCC50ToCurrent()
    {
        cc50TypesCompatibility.assertLoaded();
        testBackwardCompatibility(currentTypesCompatibility, cc50TypesCompatibility, true, EnumSet.noneOf(UpgradeDifference.class));
    }

    /**
     * {@code storage_compatibility_mode: HCD_1}, the default, keeps a rollback to the DataStax 4.0 line possible, so it
     * must relate every pair this version relates, except for the {@link #HCD_1_DOWNGRADE_DIFFERENCES}.
     * {@link LegacyTimeUUIDType} is left out: it is not a CQL type, and the DataStax 4.0 line does not have it. As in the
     * upgrade tests against the DataStax 4.0 files, the serialization relation is not compared.
     */
    @Test
    public void testMigrationFromCurrentToCC40()
    {
        cc40TypesCompatibility.assertLoaded();
        testBackwardCompatibility(cc40TypesCompatibility, currentTypesCompatibility, false, Set.of(LegacyTimeUUIDType.class), HCD_1_DOWNGRADE_DIFFERENCES);
    }

    @Test
    public void testMigrationFromCurrentToCC50()
    {
        cc50TypesCompatibility.assertLoaded();
        testBackwardCompatibility(cc50TypesCompatibility, currentTypesCompatibility, true);
    }

    private static void testBackwardCompatibility(TypesCompatibility upgradeTo, TypesCompatibility upgradeFrom, boolean serializationCompatibleWithSupported)
    {
        testBackwardCompatibility(upgradeTo, upgradeFrom, serializationCompatibleWithSupported, EnumSet.noneOf(UpgradeDifference.class));
    }

    private static void testBackwardCompatibility(TypesCompatibility upgradeTo, TypesCompatibility upgradeFrom, boolean serializationCompatibleWithSupported, Set<UpgradeDifference> expectedDifferences)
    {
        testBackwardCompatibility(upgradeTo, upgradeFrom, serializationCompatibleWithSupported, Set.of(), expectedDifferences);
    }

    /**
     * Checks that every relation {@code upgradeFrom} answers {@code true} for is still {@code true} in {@code upgradeTo},
     * except for the pairs covered by {@code expectedDifferences}, which document places where {@code upgradeTo} is
     * deliberately stricter. Each expected difference must match at least one pair, so that an exception which no
     * longer applies is noticed and removed. Types that refer to one of {@code excludedTypes} anywhere are skipped.
     */
    private static void testBackwardCompatibility(TypesCompatibility upgradeTo, TypesCompatibility upgradeFrom, boolean serializationCompatibleWithSupported, Set<Class<? extends AbstractType>> excludedTypes, Set<UpgradeDifference> expectedDifferences)
    {
        SoftAssertions assertions = new SoftAssertionsWithLimit(100);
        Map<UpgradeDifference, Integer> observedDifferences = new EnumMap<>(UpgradeDifference.class);

        assertions.assertThat(upgradeTo.knownTypes()).containsAll(Sets.difference(upgradeFrom.knownTypes(), excludedTypes));
        assertions.assertThat(upgradeTo.primitiveTypes()).containsAll(upgradeFrom.primitiveTypes().stream().filter(t -> !refersTo(t, excludedTypes)).collect(Collectors.toSet()));

        // for compatibility, we ensure that this version can read values of all the types the previous version can write
        assertions.assertThat(upgradeTo.multiCellSupportingTypesForReading()).containsAll(upgradeFrom.multiCellSupportingTypes());


        forEachTypesPair(true, (l, r) -> {
            if (l.equals(r) || refersTo(l, excludedTypes) || refersTo(r, excludedTypes))
                return;
            if (upgradeFrom.expectCompatibleWith(l, r))
                assertions.assertThat(upgradeTo.expectCompatibleWith(l, r) || isExpectedDifference(TypeRelation.COMPATIBLE, l, r, upgradeTo, expectedDifferences, observedDifferences))
                          .describedAs(isCompatibleWithDesc(l, r)).isTrue();
            if (serializationCompatibleWithSupported && upgradeFrom.expectSerializationCompatibleWith(l, r))
                assertions.assertThat(upgradeTo.expectSerializationCompatibleWith(l, r) || isExpectedDifference(TypeRelation.SERIALIZATION_COMPATIBLE, l, r, upgradeTo, expectedDifferences, observedDifferences))
                          .describedAs(isSerializationCompatibleWithDesc(l, r)).isTrue();
            if (upgradeFrom.expectValueCompatibleWith(l, r))
                assertions.assertThat(upgradeTo.expectValueCompatibleWith(l, r) || isExpectedDifference(TypeRelation.VALUE_COMPATIBLE, l, r, upgradeTo, expectedDifferences, observedDifferences))
                          .describedAs(isValueCompatibleWithDesc(l, r)).isTrue();
        });

        logger.info("Expected differences between {} and {}: {}", upgradeFrom, upgradeTo, observedDifferences);
        assertions.assertThat(observedDifferences.keySet())
                  .describedAs("expected differences between %s and %s that no type pair shows any more", upgradeFrom, upgradeTo)
                  .containsAll(expectedDifferences);
        assertions.assertAll();
    }

    private static boolean isExpectedDifference(TypeRelation relation, AbstractType<?> left, AbstractType<?> right, TypesCompatibility stricterVersion, Set<UpgradeDifference> expectedDifferences, Map<UpgradeDifference, Integer> observedDifferences)
    {
        for (UpgradeDifference difference : expectedDifferences)
        {
            if (difference.relations.contains(relation) && difference.matches(relation, left, right, stricterVersion))
            {
                observedDifferences.merge(difference, 1, Integer::sum);
                return true;
            }
        }
        return false;
    }

    private static boolean refersTo(AbstractType<?> type, Set<Class<? extends AbstractType>> classes)
    {
        return classes.contains(type.getClass()) || type.subTypes().stream().anyMatch(t -> refersTo(t, classes));
    }

    /**
     * Apache Cassandra has no multi-cell plain tuples: a tuple is always frozen, and Apache prints it as {@code TupleType(...)}
     * without a {@code FrozenType} wrapper. The DataStax type model also has multi-cell tuples, and its parser reads a bare
     * {@code TupleType(...)} as one. Sstable headers written by Apache have the same problem, and
     * {@link SerializationHeader.Component#tryFix} repairs them when they are loaded. Apply the same repair to the types read
     * from Apache's compatibility files, so that they are compared as the types Apache meant. Only for Apache's files: in the
     * DataStax files a bare {@code TupleType(...)} really is multi-cell.
     */
    private static AbstractType<?> freezeImplicitlyFrozenTuples(AbstractType<?> type)
    {
        ByteBuffer columnName = ByteBufferUtil.bytes("c");
        try
        {
            type.validateForColumn(columnName, false, false, false, false);
            return type;
        }
        catch (InvalidColumnTypeException e)
        {
            AbstractType<?> fixed = SerializationHeader.Component.tryFix(type, columnName, false, false, false, false);
            return fixed == null ? type : fixed;
        }
    }
}
