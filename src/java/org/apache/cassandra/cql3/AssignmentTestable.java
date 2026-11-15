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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.SchemaConstants;

public interface AssignmentTestable
{
    /**
     * @return whether this object can be assigned to the provided receiver. We distinguish
     * between 3 values: 
     *   - EXACT_MATCH if this object is exactly of the type expected by the receiver
     *   - WEAKLY_ASSIGNABLE if this object is not exactly the expected type but is assignable nonetheless
     *   - NOT_ASSIGNABLE if it's not assignable
     * Most caller should just call the isAssignable() method on the result, though functions have a use for
     * testing "strong" equality to decide the most precise overload to pick when multiple could match.
     */
    public TestResult testAssignment(String keyspace, ColumnSpecification receiver);

    /**
     * @return A data type that can represent this, or {@code null} if we can't determine that type. The returned type
     * won't necessarely be the exact type, but one that is compatible with it.
     */
    @Nullable
    public AbstractType<?> getCompatibleTypeIfKnown(String keyspace);

    /**
     * @return A data type that can represent all the specified types, or {@code null} if there isn't one. If several
     * types qualify, the first one in the iteration order of {@code types} is returned, so pass an ordered collection
     * for a deterministic result.
     */
    @Nullable
    public static AbstractType<?> getCompatibleTypeIfKnown(Collection<AbstractType<?>> types)
    {
        return types.stream()
                    .filter(type -> types.stream().allMatch(t -> t.testAssignment(type).isAssignable()))
                    .findFirst()
                    .orElse(null);
    }

    /**
     * Tests whether this can be assigned to a receiver whose type is the type inferred from this with
     * {@link #getCompatibleTypeIfKnown}, like the parameter types of a function built by a
     * {@link org.apache.cassandra.cql3.functions.FunctionFactory}.
     * <p>
     * The type inferred for a collection literal has a frozen element type, because a frozen type can't have non-frozen
     * subtypes, while the type inferred from its elements may be non-frozen, like {@code t} for {@code [v, v]} with
     * {@code v} a non-frozen UDT. The collection literals override this to test their elements against the latter.
     */
    default TestResult testAssignmentToInferredType(String keyspace, ColumnSpecification receiver)
    {
        return testAssignment(keyspace, receiver);
    }

    /**
     * Tests the elements of a collection literal against the element type of a receiver whose type is the type inferred
     * from the literal (see {@link #testAssignmentToInferredType}). If the receiver's element type is the frozen form of
     * the type inferred from the elements, the elements are tested against the latter; an element of another type, like
     * a tuple next to a UDT, is tested against the receiver's element type. A nested literal is tested in the same way
     * only against its own inferred type.
     */
    static TestResult testAllToInferredType(String keyspace,
                                            ColumnSpecification elementSpec,
                                            Collection<? extends AssignmentTestable> elements)
    {
        Set<AbstractType<?>> types = elements.stream()
                                             .filter(Objects::nonNull)
                                             .map(AssignmentTestable::inferredType)
                                             .filter(Objects::nonNull)
                                             .collect(Collectors.toCollection(LinkedHashSet::new));
        AbstractType<?> type = getCompatibleTypeIfKnown(types);
        ColumnSpecification inferredSpec = elementSpec;
        if (type != null && type.isMultiCell() && type.freeze().equals(elementSpec.type))
            inferredSpec = new ColumnSpecification(elementSpec.ksName, elementSpec.cfName, elementSpec.name, type);

        TestResult res = TestResult.EXACT_MATCH;
        for (AssignmentTestable element : elements)
        {
            if (element == null)
            {
                res = TestResult.WEAKLY_ASSIGNABLE;
                continue;
            }

            TestResult elementRes = testElementToInferredType(keyspace, element, inferredSpec);
            if (elementRes == TestResult.NOT_ASSIGNABLE
                && inferredSpec != elementSpec
                && !isOfFrozenType(element, elementSpec.type))
                elementRes = testElementToInferredType(keyspace, element, elementSpec);

            if (elementRes == TestResult.NOT_ASSIGNABLE)
                return TestResult.NOT_ASSIGNABLE;

            if (elementRes == TestResult.WEAKLY_ASSIGNABLE)
                res = TestResult.WEAKLY_ASSIGNABLE;
        }
        return res;
    }

    private static TestResult testElementToInferredType(String keyspace,
                                                        AssignmentTestable element,
                                                        ColumnSpecification spec)
    {
        AbstractType<?> type = inferredType(element);
        return type != null && type.equals(spec.type)
               ? element.testAssignmentToInferredType(keyspace, spec)
               : element.testAssignment(keyspace, spec);
    }

    private static AbstractType<?> inferredType(AssignmentTestable testable)
    {
        return testable.getCompatibleTypeIfKnown(SchemaConstants.SYSTEM_KEYSPACE_NAME);
    }

    private static boolean isOfFrozenType(AssignmentTestable element, AbstractType<?> frozenType)
    {
        AbstractType<?> type = inferredType(element);
        return type != null && type.freeze().equals(frozenType);
    }

    public enum TestResult
    {
        EXACT_MATCH, WEAKLY_ASSIGNABLE, NOT_ASSIGNABLE;

        public boolean isAssignable()
        {
            return this != NOT_ASSIGNABLE;
        }

        public boolean isExactMatch()
        {
            return this == EXACT_MATCH;
        }

        // Test all elements of toTest for assignment. If all are exact match, return exact match. If any is not assignable,
        // return not assignable. Otherwise, return weakly assignable.
        public static TestResult testAll(String keyspace, ColumnSpecification receiver, Collection<? extends AssignmentTestable> toTest)
        {
            TestResult res = EXACT_MATCH;
            for (AssignmentTestable rt : toTest)
            {
                if (rt == null)
                {
                    res = WEAKLY_ASSIGNABLE;
                    continue;
                }

                TestResult t = rt.testAssignment(keyspace, receiver);
                if (t == NOT_ASSIGNABLE)
                    return NOT_ASSIGNABLE;

                if (t == WEAKLY_ASSIGNABLE)
                    res = WEAKLY_ASSIGNABLE;
            }
            return res;
        }
    }
}
