/**
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

package org.apache.hadoop.metrics2.sink.kafka;

import java.io.Closeable;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * this is a copy of ScheduledReporter, except it replaces MetricsRegistry with
 * Map<MetricName, Metric>. This is a dirty quick prototype. Probably should
 * reference PushHttpMetricsReporter.java for real implementation.
 */

public abstract class KafkaclientScheduledReporter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaclientScheduledReporter.class);

    /**
     * A simple named thread factory.
     */
    @SuppressWarnings("NullableProblems")
    private static class NamedThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        private NamedThreadFactory(String name) {
            final SecurityManager s = System.getSecurityManager();
            this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = "metrics-" + name + "-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            final Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            t.setDaemon(true);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

    private static final AtomicInteger FACTORY_ID = new AtomicInteger();

    private final Map<org.apache.kafka.common.MetricName, KafkaMetric> metrics;
    private final ScheduledExecutorService executor;
    private final double durationFactor;
    private final String durationUnit;
    private final double rateFactor;
    private final String rateUnit;

    /**
     * Creates a new {@link ScheduledReporter} instance.
     *
     * @param metrics
     *          Kafka metrics
     * @param name
     *          the reporter's name
     * @param rateUnit
     *          a unit of time
     * @param durationUnit
     *          a unit of time
     */
    protected KafkaclientScheduledReporter(Map<MetricName, KafkaMetric> metrics, String name, TimeUnit rateUnit, TimeUnit durationUnit) {
        this(metrics, rateUnit, durationUnit, Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(name + '-'
                + FACTORY_ID.incrementAndGet())));
    }

    /**
     * Creates a new {@link ScheduledReporter} instance.
     *
     * @param metrics
     *          Kafka metrics
     * @param executor
     *          the executor to use while scheduling reporting of metrics.
     */
    protected KafkaclientScheduledReporter(Map<MetricName, KafkaMetric> metrics, TimeUnit rateUnit, TimeUnit durationUnit,
                                ScheduledExecutorService executor) {
        this.metrics = metrics;
        this.executor = executor;
        this.rateFactor = rateUnit.toSeconds(1);
        this.rateUnit = calculateRateUnit(rateUnit);
        this.durationFactor = 1.0 / durationUnit.toNanos(1);
        this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
    }

    /**
     * Starts the reporter polling at the given period.
     *
     * @param period
     *          the amount of time between polls
     * @param unit
     *          the unit for {@code period}
     */
    public void start(long period, TimeUnit unit) {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    report();
                } catch (RuntimeException ex) {
                    LOG.error("RuntimeException thrown from {}#report. Exception was suppressed.", this
                            .getClass().getSimpleName(), ex);
                }
            }
        }, period, period, unit);
    }

    /**
     * Stops the reporter and shuts down its thread of execution.
     *
     * Uses the shutdown pattern from
     * http://docs.oracle.com/javase/7/docs/api/java
     * /util/concurrent/ExecutorService.html
     */
    public void stop() {
        executor.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    System.err.println(getClass().getSimpleName() + ": ScheduledExecutorService did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stops the reporter and shuts down its thread of execution.
     */
    @Override
    public void close() {
        stop();
    }

    /**
     * Report the current values of all metrics in the registry.
     */
    public void report() {
        synchronized (this) {
            report(metrics.entrySet());
        }
    }

    /**
     * Called periodically by the polling thread. Subclasses should report all the
     * given metrics.
     *
     * @param metrics
     *          all the metrics in the registry
     */
    public abstract void report(Set<Entry<MetricName, KafkaMetric>> metrics);

    protected String getRateUnit() {
        return rateUnit;
    }

    protected String getDurationUnit() {
        return durationUnit;
    }

    protected double convertDuration(double duration) {
        return duration * durationFactor;
    }

    protected double convertRate(double rate) {
        return rate * rateFactor;
    }

    private String calculateRateUnit(TimeUnit unit) {
        final String s = unit.toString().toLowerCase(Locale.US);
        return s.substring(0, s.length() - 1);
    }

}
