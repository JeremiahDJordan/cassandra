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

package org.apache.cassandra.schema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ClassLoadingTestNonAssignable;
import org.apache.cassandra.utils.ClassLoadingTestSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MemtableParamsTest
{
    @Test
    public void testInvalidFactoryMethodDoesNotInitializeClass() throws Exception
    {
        ClassLoadingTestSupport.assertNotInitialized(MemtableFactoryInvalidReturnType.class);

        assertThatThrownBy(() -> getMemtableFactory(MemtableFactoryInvalidReturnType.class))
        .isInstanceOf(ConfigurationException.class)
        .hasStackTraceContaining("must return");

        assertThat(ClassLoadingTestSupport.wasInitialized(MemtableFactoryInvalidReturnType.class)).isFalse();
    }

    @Test
    public void testInvalidFactoryFieldDoesNotInitializeClass() throws Exception
    {
        ClassLoadingTestSupport.assertNotInitialized(MemtableFactoryInvalidFieldType.class);

        assertThatThrownBy(() -> getMemtableFactory(MemtableFactoryInvalidFieldType.class))
        .isInstanceOf(ConfigurationException.class)
        .hasStackTraceContaining("must be of type");

        assertThat(ClassLoadingTestSupport.wasInitialized(MemtableFactoryInvalidFieldType.class)).isFalse();
    }

    @Test
    public void testNonMemtableClassIsNotInitialized() throws Exception
    {
        ClassLoadingTestSupport.assertNotInitialized(ClassLoadingTestNonAssignable.class);

        assertThatThrownBy(() -> getMemtableFactory(ClassLoadingTestNonAssignable.class))
        .isInstanceOf(ConfigurationException.class);

        assertThat(ClassLoadingTestSupport.wasInitialized(ClassLoadingTestNonAssignable.class)).isFalse();
    }

    private static void getMemtableFactory(Class<?> memtableClass) throws Exception
    {
        Method method = MemtableParams.class.getDeclaredMethod("getMemtableFactory", Map.class);
        method.setAccessible(true);
        try
        {
            method.invoke(null, ImmutableMap.of(MemtableParams.Option.CLASS.toString(), memtableClass.getName()));
        }
        catch (InvocationTargetException e)
        {
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException) e.getCause();
            throw e;
        }
    }
}
