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
package org.apache.cassandra.inject;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HeapUtils;
import org.jboss.byteman.agent.install.Install;
import org.jboss.byteman.agent.submit.Submit;
import org.jboss.byteman.rule.helper.Helper;

import static org.apache.cassandra.inject.ActionBuilder.newActionBuilder;
import static org.apache.cassandra.inject.Expression.expr;
import static org.apache.cassandra.inject.Expression.method;
import static org.apache.cassandra.inject.Expression.quote;
import static org.assertj.core.api.Assertions.assertThat;

public class Injections
{
    private static Submit submitter;

    public static void inject(Injection...injections) throws Throwable
    {
        String script = Arrays.stream(injections).map(Injection::format).collect(Collectors.joining("\n"));
        getSubmitter().addRulesFromResources(Lists.newArrayList(IOUtils.toInputStream(script)));
    }

    public static void deleteAll()
    {
        try
        {
            getSubmitter().deleteAllRules();
        }
        catch (Throwable ignore)
        {
            // Ignore because it will throw if there aren't any injections
        }
    }

    private static Submit getSubmitter() throws Throwable
    {
        if (submitter == null)
        {
            submitter = new Submit(FBUtilities.getBroadcastAddressAndPort().address.getHostAddress(), loadAgent());
        }
        return submitter;
    }

    private static int loadAgent() throws Throwable
    {
        int port = getPort();
        long pid = HeapUtils.getProcessId();
        List<String> properties = new ArrayList<>();
        properties.add("org.jboss.byteman.transform.all=true");
        // uncomment below two lines to add debug or/and more verbose info, if you need to debug an injection
        // properties.add("org.jboss.byteman.verbose=true");
        // properties.add("org.jboss.byteman.debug=true");
        Install.install(Long.toString(pid), true, true, FBUtilities.getBroadcastAddressAndPort().address.getHostAddress(), port, properties.toArray(new String[0]));
        return port;
    }

    private static int getPort()
    {
        try (ServerSocket serverSocket = new ServerSocket(0))
        {
            return serverSocket.getLocalPort();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface CallMe {}

    public abstract static class MultiInvokePointInjectionBuilder<T extends Injection, B extends InjectionBuilder<T>>
            extends InjectionBuilder<T>
    {
        final List<InvokePointBuilder> invokePointBuilders = new LinkedList<>();

        MultiInvokePointInjectionBuilder(String id)
        {
            super(id);
        }

        /**
         * Adds a new invoke point to the injection. Adding a new invoke point cause creation of a new rule because
         * a single rule can only have a single invoke point.
         */
        public B add(InvokePointBuilder ipb)
        {
            invokePointBuilders.add(ipb);
            return (B) this;
        }
    }

    public abstract static class CrossProductInjectionBuilder<T extends Injection, B extends InjectionBuilder<T>>
            extends MultiInvokePointInjectionBuilder<T, B>
    {
        final LinkedList<ActionBuilder> actionBuilders = new LinkedList<>();

        CrossProductInjectionBuilder(String id)
        {
            super(id);
        }

        Rule[] getRules()
        {
            Rule[] rules = new Rule[actionBuilders.size() * invokePointBuilders.size()];
            int i = 0;
            for (InvokePointBuilder ipb : invokePointBuilders)
            {
                for (ActionBuilder ab : actionBuilders)
                {
                    rules[i++] = Rule.newRule(id + "_" + i, ab, ipb);
                }
            }
            return rules;
        }

        /**
         * Adds a new action to the injection. Adding a new action cause creation of a new rule because a single rule
         * can only have a single action. Do not confuse an action with a statement - an action is bundle of bindings,
         * condition under which it can be invoked and a sequence of statements.
         *
         * If you just need to add a statement to the existing action, see {@link #withLastActionBuilder(Consumer)} and
         * {@link #lastActionBuilder()}.
         */
        public B add(ActionBuilder builder)
        {
            if (builder.getHelperClass() == null)
            {
                builder.withHelperClass(Helper.class);
            }
            builder.conditions().when(getIsEnabledExpression());
            actionBuilders.add(builder);
            return (B) this;
        }

        /**
         * @see #add(ActionBuilder)
         */
        public B add(ActionBuilder.Builder builder)
        {
            return add(builder.toActionBuilder());
        }

        /**
         * Allows to modify the last defined action. You can add new bindings, conditions and statements. If you need
         * to create a new action, please see {@link #add(ActionBuilder)}.
         */
        public ActionBuilder lastActionBuilder()
        {
            Preconditions.checkState(!actionBuilders.isEmpty());
            ActionBuilder ab = actionBuilders.getLast();
            return ab;
        }

        /**
         * @see #lastActionBuilder()
         */
        public B withLastActionBuilder(Consumer<ActionBuilder> builder)
        {
            Preconditions.checkState(!actionBuilders.isEmpty());
            ActionBuilder ab = actionBuilders.getLast();
            builder.accept(ab);
            return (B) this;
        }
    }

    /**
     * Creates a new {@link Counter} injection.
     *
     * @param name name of the internal counter
     */
    public static Counter.CounterBuilder newCounter(String name)
    {
        return new Counter.CounterBuilder(name);
    }

    /**
     * Creates an injection along with a distributed counter. It increments the counter whenever an invoke point
     * is reached. You can add multiple invoke points and you can also bundle additional actions - for example,
     * throw an exception and count how many times it happened.
     */
    public static class Counter extends Injection
    {
        private static Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
        private final String name;
        private final AtomicLong internalCounter;

        private Counter(String id, String name, Rule[] rules)
        {
            super(id, rules);
            this.name = name;
            this.internalCounter = counters.computeIfAbsent(name, n -> new AtomicLong());
            reset();
        }

        /**
         * Get a current value of the counter.
         */
        public long get()
        {
            return internalCounter.get();
        }

        /**
         * Reset counter value to 0.
         */
        public void reset()
        {
            internalCounter.set(0);
        }

        @CallMe
        public static void increment(String name)
        {
            counters.get(name).incrementAndGet();
        }

        public static class CounterBuilder extends CrossProductInjectionBuilder<Counter, CounterBuilder>
        {
            private final String name;

            private CounterBuilder(String name)
            {
                super(String.format("counter/%s/%s", name, UUID.randomUUID().toString()));
                this.name = name;
                add(newActionBuilder().actions().doAction(method(Counter.class, CallMe.class).args(quote(name))));
            }

            @Override
            public Counter build()
            {
                return new Counter(id, name, getRules());
            }
        }
    }

    /**
     * Creates a new {@link Barrier} injection.
     *
     * @param name name of the barrier
     * @param parties how many parties the barrier should wait for
     * @param cyclic whether the barrier should reset after resume
     */
    public static Barrier.BarrierBuilder newBarrier(String name, int parties, boolean cyclic)
    {
        return new Barrier.BarrierBuilder(name, parties, cyclic, true, true);
    }

    /**
     * Creates a {@link Barrier} injection point where the number of parties is just counted down but it does not await.
     *
     * @param name name of the barrier
     * @param parties how many parties a new barrier should wait for
     * @param cyclic whether the barrier should reset after resume
     */
    public static Barrier.BarrierBuilder newBarrierCountDown(String name, int parties, boolean cyclic)
    {
        return new Barrier.BarrierBuilder(name, parties, cyclic, true, false);
    }

    /**
     * Creates a {@link Barrier} injection point where it just awaits but it does not decrement the number of parties
     * it is awaiting for.
     *
     * @param name name of the barrier
     * @param parties how many parties a new barrier should wait for
     * @param cyclic whether the barrier should reset after resume
     */
    public static Barrier.BarrierBuilder newBarrierAwait(String name, int parties, boolean cyclic)
    {
        return new Barrier.BarrierBuilder(name, parties, cyclic, false, true);
    }

    /**
     * Creates an injection with a distributed barrier awaiting for a defined number of parties to reach it
     * (including test node). It can be used to synchronize multiple nodes
     */
    public static class Barrier extends Injection
    {
        private static Map<String, CyclicBarrier> barriers = new ConcurrentHashMap<>();
        private final boolean doCountDown;
        private final boolean doAwait;
        private final CyclicBarrier internalBarrier;

        private Barrier(String id, String name, int parties, boolean cyclic, boolean doCountDown, boolean doAwait, Rule[] rules)
        {
            super(id, rules);
            this.internalBarrier = barriers.computeIfAbsent(name, n -> new CyclicBarrier(name, parties, cyclic));
            this.doCountDown = doCountDown;
            this.doAwait = doAwait;
            reset();
        }

        /**
         * Do a single step for this barrier that is, decrement the number of parties and await, depending on which
         * actions are enabled. I may just decrement the number of parties, without awaiting as well as it may just
         * await, without decrementing the number of parties.
         */
        public void arrive() throws InterruptedException
        {
            internalBarrier.await(doCountDown, doAwait);
        }

        /**
         * Just wait for this barrier to be released. Do not decrement the number of parties regardless of how this
         * barrier was setup - this is force await.
         */
        public void await() throws InterruptedException
        {
            internalBarrier.await(false, true);
        }

        /**
         * Just decrement the number of parties this barrier is waiting for but do not wait regardless of how
         * this barrier was setup - this is force countdown.
         */
        public void countDown()
        {
            try
            {
                internalBarrier.await(true, false);
            }
            catch (InterruptedException ignored)
            {
                // will not happen in case just a countdown
            }
        }

        /**
         * Get the number of parties the barrier is currently awaiting for.
         */
        public long getCount()
        {
            return internalBarrier.getCount();
        }

        /**
         * Reset the barrier, that is, let all waiting threads resume and reset the number of parties
         * (if cyclic is enabled).
         */
        public void reset()
        {
            internalBarrier.reset();
        }

        @CallMe
        public static void doAction(String name, boolean doCountDown, boolean doAwait) throws InterruptedException
        {
            CyclicBarrier barrier = barriers.get(name);
            barrier.await(doCountDown, doAwait);
        }

        public static class BarrierBuilder extends CrossProductInjectionBuilder<Barrier, BarrierBuilder>
        {
            private final String name;
            private final int parties;
            private final boolean cyclic;
            private final boolean doAwait;
            private final boolean doCountDown;

            private BarrierBuilder(String name, int parties, boolean cyclic, boolean doCountDown, boolean doAwait)
            {
                super(String.format("barrier/%s/%s", name, UUID.randomUUID().toString()));
                this.name = name;
                this.parties = parties;
                this.cyclic = cyclic;
                this.doAwait = doAwait;
                this.doCountDown = doCountDown;
                add(newActionBuilder().actions().doAction(method(Barrier.class, CallMe.class)
                        .args(quote(name), doCountDown, doAwait)));
            }

            @Override
            public Barrier build()
            {
                return new Barrier(id, name, parties, cyclic, doCountDown, doAwait, getRules());
            }
        }
    }

    /**
     * Creates a new {@link Hold} injection, which gives up waiting after {@link Hold#DEFAULT_TIMEOUT_SECONDS}.
     *
     * @param name name of the hold, in its messages
     */
    public static Hold.HoldBuilder newHold(String name)
    {
        return newHold(name, Hold.DEFAULT_TIMEOUT_SECONDS);
    }

    /**
     * Creates a new {@link Hold} injection.
     *
     * @param name name of the hold, in its messages
     * @param timeoutSeconds how long the held call waits for the release at most
     */
    public static Hold.HoldBuilder newHold(String name, long timeoutSeconds)
    {
        return new Hold.HoldBuilder(name, timeoutSeconds);
    }

    /**
     * Creates an injection that holds the first call to reach it until the test releases it. Later calls go on
     * without waiting, and don't release it. The held call gives up waiting after a timeout, which makes the release
     * fail, as does an interruption of the held call or a second release.
     */
    public static class Hold extends Injection
    {
        public static final long DEFAULT_TIMEOUT_SECONDS = 60;
        // by injection id: holds of the same name are distinct
        private static final Map<String, Hold> holds = new ConcurrentHashMap<>();
        private final String name;
        private final long timeoutSeconds;
        private final AtomicBoolean taken = new AtomicBoolean();
        private final AtomicBoolean releasing = new AtomicBoolean();
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final CountDownLatch left = new CountDownLatch(1);
        private volatile Thread thread;
        private volatile boolean held;
        private volatile boolean interrupted;
        private volatile boolean expired;

        private Hold(String id, String name, long timeoutSeconds, Rule[] rules)
        {
            super(id, rules);
            this.name = name;
            this.timeoutSeconds = timeoutSeconds;
            holds.put(id, this);
        }

        /**
         * Waits for a call to reach the hold, and checks it is still held.
         */
        public void awaitHeld() throws InterruptedException
        {
            assertThat(reached.await(60, TimeUnit.SECONDS)).as("hold %s reached", name).isTrue();
            assertHeld();
        }

        /**
         * Checks the call the hold took is still held: nothing has released it, and it hasn't given up waiting.
         */
        public void assertHeld()
        {
            assertThat(held && !interrupted && !expired && released.getCount() == 1).as("hold %s held", name).isTrue();
        }

        /**
         * Releases the call the hold took, once, and waits for it to leave its wait released rather than given up.
         */
        public void release() throws InterruptedException
        {
            assertThat(releasing.compareAndSet(false, true)).as("hold %s released once", name).isTrue();
            assertHeld();
            released.countDown();
            assertThat(left.await(60, TimeUnit.SECONDS)).as("hold %s left", name).isTrue();
            assertThat(expired || interrupted).as("hold %s given up", name).isFalse();
        }

        /**
         * The thread of the call the hold took, if any.
         */
        public Thread heldThread()
        {
            return thread;
        }

        /**
         * Releases every hold, whether or not a call reached it, without checking or waiting (cleanup after a failure
         * too), and forgets them.
         */
        public static void releaseAll()
        {
            holds.values().forEach(hold -> hold.released.countDown());
            holds.clear();
        }

        /**
         * Holds the calling thread until the release, if it is the first to arrive; returns at once otherwise.
         */
        public void arrive() throws InterruptedException
        {
            if (!taken.compareAndSet(false, true))
                return;
            thread = Thread.currentThread();
            held = true;
            reached.countDown();
            try
            {
                expired = !released.await(timeoutSeconds, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                interrupted = true;
                throw e;
            }
            finally
            {
                held = false;
                left.countDown();
            }
        }

        @CallMe
        public static void doAction(String id) throws InterruptedException
        {
            Hold hold = holds.get(id);
            if (hold != null)
                hold.arrive();
        }

        public static class HoldBuilder extends CrossProductInjectionBuilder<Hold, HoldBuilder>
        {
            private final String name;
            private final long timeoutSeconds;

            private HoldBuilder(String name, long timeoutSeconds)
            {
                super(String.format("hold/%s/%s", name, UUID.randomUUID()));
                this.name = name;
                this.timeoutSeconds = timeoutSeconds;
                add(newActionBuilder().actions().doAction(method(Hold.class, CallMe.class).args(quote(id))));
            }

            @Override
            public Hold build()
            {
                return new Hold(id, name, timeoutSeconds, getRules());
            }
        }
    }

    /**
     * Creates {@link Times} injection.
     *
     * @param name name of the internal counter
     * @param defaultTimes the number of times the action should be executed
     */
    public static Times.TimesBuilder newTimes(String name, int defaultTimes)
    {
        return new Times.TimesBuilder(name, defaultTimes);
    }

    /**
     * Creates an injection which allows to invoke a defined action for a defined number of times.
     */
    public static class Times extends Injection
    {
        private static Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
        private final AtomicLong internalCounter;
        private final int defaultTimes;

        private Times(String id, String name, int defaultTimes, Rule[] rules)
        {
            super(id, rules);
            this.internalCounter = counters.computeIfAbsent(name, n -> new AtomicLong(defaultTimes));
            this.defaultTimes = defaultTimes;
            reset();
        }

        /**
         * Get the remaining number of times the action will be attempted to be executed.
         */
        public long get()
        {
            return internalCounter.get();
        }

        /**
         * Reset the internal counter to the original value.
         */
        public void reset()
        {
            reset(defaultTimes);
        }

        /**
         * Reset the internal counter to the given value.
         */
        public void reset(int n)
        {
            internalCounter.set(n);
        }

        @CallMe
        public static boolean decrementAndCheck(String name)
        {
            AtomicLong counter = counters.get(name);
            long value = counter.decrementAndGet();
            return value >= 0;
        }

        public static class TimesBuilder extends CrossProductInjectionBuilder<Times, TimesBuilder>
        {
            private final String name;
            private final int defaultTimes;

            private TimesBuilder(String name, int defaultTimes)
            {
                super(String.format("times/%s/%s", name, UUID.randomUUID().toString()));
                this.name = name;
                this.defaultTimes = defaultTimes;
            }

            @Override
            public TimesBuilder add(ActionBuilder builder)
            {
                super.add(builder);
                builder.conditions().when(method(Times.class, CallMe.class).args(quote(name)));
                return this;
            }

            @Override
            public Times build()
            {
                return new Times(id, name, defaultTimes, getRules());
            }
        }
    }

    public abstract static class SingleActionBuilder<T extends Injection, B extends InjectionBuilder<T>>
            extends MultiInvokePointInjectionBuilder<T, B>
    {
        protected final ActionBuilder actionBuilder = newActionBuilder();

        public SingleActionBuilder(String id)
        {
            super(id);
            actionBuilder.conditions().when(getIsEnabledExpression());
        }

        public B action(Consumer<ActionBuilder> builder)
        {
            builder.accept(actionBuilder);
            return (B) this;
        }

        protected Rule[] getRules()
        {
            Rule[] rules = new Rule[invokePointBuilders.size()];
            int i = 0;
            for (InvokePointBuilder ipb : invokePointBuilders)
            {
                rules[i++] = Rule.newRule(id + "_" + i, actionBuilder, ipb);
            }
            return rules;
        }
    }

    /**
     * Creates a custom injection which allows you to define your invoke points and actions from scratch.
     *
     * @param name name of the injection
     */
    public static CustomBuilder newCustom(String name)
    {
        return new CustomBuilder(name);
    }

    public static class CustomBuilder extends CrossProductInjectionBuilder<Injection, CustomBuilder>
    {
        public CustomBuilder(String name)
        {
            super(String.format("custom/%s/%s", name, UUID.randomUUID().toString()));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Injection build()
        {
            return new Injection(id, getRules()) {};
        }
    }

    /**
     * Creates a pause to hold on the thread for a defined amount of time.
     *
     * @param name name of the pause
     * @param timeout time out in milliseconds
     */
    public static PauseBuilder newPause(String name, long timeout)
    {
        return new PauseBuilder(name, timeout);
    }

    public static class PauseBuilder extends SingleActionBuilder<Injection, PauseBuilder>
    {
        public PauseBuilder(String name, long timeout)
        {
            super(String.format("pause/%s/%s", name, UUID.randomUUID().toString()));
            actionBuilder.actions().doAction(expr("Thread.sleep").args(timeout+"L"));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Injection build()
        {
            return new Injection(id, getRules());
        }
    }
}
