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

import kafka.metrics.KafkaMetricsConfig;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.sink.timeline.AbstractTimelineMetricsSink;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetrics;
import org.apache.hadoop.metrics2.sink.timeline.cache.TimelineMetricsCache;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import static org.apache.hadoop.metrics2.sink.timeline.cache.TimelineMetricsCache.MAX_EVICTION_TIME_MILLIS;
import static org.apache.hadoop.metrics2.sink.timeline.cache.TimelineMetricsCache.MAX_RECS_PER_NAME_DEFAULT;

public class KafkaclientTimelineMetricsReporter extends AbstractTimelineMetricsSink
        implements MetricsReporter, KafkaclientTimelineMetricsReporterMBean {

    private final static Log LOG = LogFactory.getLog(KafkaclientTimelineMetricsReporter.class);

    private static final String TIMELINE_METRICS_KAFKACLIENT_PREFIX = "kafkaclient.timeline.metrics.";
    private static final String TIMELINE_METRICS_KAFKACLIENT_ID = TIMELINE_METRICS_KAFKACLIENT_PREFIX + "client.id";
    private static final String TIMELINE_METRICS_SEND_INTERVAL_PROPERTY = "sendInterval";
    private static final String TIMELINE_METRICS_MAX_ROW_CACHE_SIZE_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + "maxRowCacheSize";
    private static final String TIMELINE_HOSTS_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + "hosts";
    private static final String TIMELINE_PORT_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + "port";
    private static final String TIMELINE_PROTOCOL_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + "protocol";
    private static final String TIMELINE_REPORTER_ENABLED_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + "reporter.enabled";
    private static final String TIMELINE_METRICS_SSL_KEYSTORE_PATH_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + SSL_KEYSTORE_PATH_PROPERTY;
    private static final String TIMELINE_METRICS_SSL_KEYSTORE_TYPE_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + SSL_KEYSTORE_TYPE_PROPERTY;
    private static final String TIMELINE_METRICS_SSL_KEYSTORE_PASSWORD_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + SSL_KEYSTORE_PASSWORD_PROPERTY;
    private static final String TIMELINE_METRICS_KAFKA_INSTANCE_ID_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + INSTANCE_ID_PROPERTY;
    private static final String TIMELINE_METRICS_KAFKA_SET_INSTANCE_ID_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + SET_INSTANCE_ID_PROPERTY;
    private static final String TIMELINE_METRICS_KAFKA_HOST_IN_MEMORY_AGGREGATION_ENABLED_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + HOST_IN_MEMORY_AGGREGATION_ENABLED_PROPERTY;
    private static final String TIMELINE_METRICS_KAFKA_HOST_IN_MEMORY_AGGREGATION_PORT_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + HOST_IN_MEMORY_AGGREGATION_PORT_PROPERTY;
    private static final String TIMELINE_METRICS_KAFKA_HOST_IN_MEMORY_AGGREGATION_PROTOCOL_PROPERTY = TIMELINE_METRICS_KAFKACLIENT_PREFIX + HOST_IN_MEMORY_AGGREGATION_PROTOCOL_PROPERTY;
    private static final String TIMELINE_DEFAULT_HOST = "localhost";
    private static final String TIMELINE_DEFAULT_PORT = "6188";
    private static final String TIMELINE_DEFAULT_PROTOCOL = "http";
    private static final String EXCLUDED_METRICS_PROPERTY = "external.kafka.metrics.exclude.prefix";
    private static final String INCLUDED_METRICS_PROPERTY = "external.kafka.metrics.include.prefix";

    private volatile boolean initialized = false;
    private boolean running = false;
    private final Object reporterLock = new Object();
    private String hostname;
    private String clientId;
    private String metricCollectorPort;
    private Collection<String> collectorHosts;
    private String metricCollectorProtocol;
    private KafkaclientTimelineMetricsReporter.TimelineScheduledReporter reporter;
    private TimelineMetricsCache metricsCache;
    private int timeoutSeconds = 10;
    private String zookeeperQuorum = null;
    private boolean setInstanceId;
    private String instanceId;

    private String[] excludedMetricsPrefixes;
    private String[] includedMetricsPrefixes;
    // Local cache to avoid prefix matching everytime
    private Set<String> excludedMetrics = new HashSet<>();
    private boolean hostInMemoryAggregationEnabled;
    private int hostInMemoryAggregationPort;
    private String hostInMemoryAggregationProtocol;

    public KafkaclientTimelineMetricsReporter() {
        LOG.info("create KafkaclientTimelineMetricsReporter instance",
                new Exception("new KafkaclientTimelineMetricsReporter"));
    }

    @Override
    protected String getCollectorUri(String host) {
        return constructTimelineMetricUri(metricCollectorProtocol, host, metricCollectorPort);
    }

    @Override
    protected String getCollectorProtocol() {
        return metricCollectorProtocol;
    }

    @Override
    protected String getCollectorPort() {
        return metricCollectorPort;
    }

    @Override
    protected int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    @Override
    protected String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    @Override
    protected Collection<String> getConfiguredCollectorHosts() {
        return collectorHosts;
    }

    @Override
    protected String getHostname() {
        return hostname;
    }

    @Override
    protected boolean isHostInMemoryAggregationEnabled() {
        return hostInMemoryAggregationEnabled;
    }

    @Override
    protected int getHostInMemoryAggregationPort() {
        return hostInMemoryAggregationPort;
    }

    @Override
    protected String getHostInMemoryAggregationProtocol() {
        return hostInMemoryAggregationProtocol;
    }

    public void setMetricsCache(TimelineMetricsCache metricsCache) {
        this.metricsCache = metricsCache;
    }

    private void init(VerifiableProperties props) {
        synchronized (reporterLock) {
            if (!initialized) {
                LOG.info("Initializing Kafka client Timeline Metrics Sink");
                try {
                    hostname = InetAddress.getLocalHost().getHostName();
                    //If not FQDN , call  DNS
                    if ((hostname == null) || (!hostname.contains("."))) {
                        hostname = InetAddress.getLocalHost().getCanonicalHostName();
                    }
                } catch (UnknownHostException e) {
                    LOG.error("Could not identify hostname.");
                    throw new RuntimeException("Could not identify hostname.", e);
                }
                // Initialize the collector write strategy
                super.init();

                KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);
                clientId = props.getString(TIMELINE_METRICS_KAFKACLIENT_ID, "kafka_client");
                LOG.info(TIMELINE_METRICS_KAFKACLIENT_ID + " = " + clientId);

                timeoutSeconds = props.getInt(METRICS_POST_TIMEOUT_SECONDS, DEFAULT_POST_TIMEOUT_SECONDS);
                int metricsSendInterval = props.getInt(TIMELINE_METRICS_SEND_INTERVAL_PROPERTY, MAX_EVICTION_TIME_MILLIS);
                int maxRowCacheSize = props.getInt(TIMELINE_METRICS_MAX_ROW_CACHE_SIZE_PROPERTY, MAX_RECS_PER_NAME_DEFAULT);
                LOG.info(METRICS_POST_TIMEOUT_SECONDS + " = " + timeoutSeconds);
                LOG.info(TIMELINE_METRICS_SEND_INTERVAL_PROPERTY + " = " + metricsSendInterval);
                LOG.info(TIMELINE_METRICS_MAX_ROW_CACHE_SIZE_PROPERTY + " = " + maxRowCacheSize);

                zookeeperQuorum = props.containsKey(COLLECTOR_ZOOKEEPER_QUORUM) ?
                        props.getString(COLLECTOR_ZOOKEEPER_QUORUM) : props.getString("zookeeper.connect");
                LOG.info("contains " + COLLECTOR_ZOOKEEPER_QUORUM + ": " + props.containsKey(COLLECTOR_ZOOKEEPER_QUORUM));

                metricCollectorPort = props.getString(TIMELINE_PORT_PROPERTY, TIMELINE_DEFAULT_PORT);
                collectorHosts = parseHostsStringIntoCollection(props.getString(TIMELINE_HOSTS_PROPERTY, TIMELINE_DEFAULT_HOST));
                metricCollectorProtocol = props.getString(TIMELINE_PROTOCOL_PROPERTY, TIMELINE_DEFAULT_PROTOCOL);
                LOG.info(TIMELINE_PORT_PROPERTY + " = " + metricCollectorPort);
                LOG.info(TIMELINE_HOSTS_PROPERTY + " = " + collectorHosts);
                LOG.info(TIMELINE_PROTOCOL_PROPERTY + " = " + metricCollectorProtocol);

                instanceId = props.getString(TIMELINE_METRICS_KAFKA_INSTANCE_ID_PROPERTY, null);
                setInstanceId = props.getBoolean(TIMELINE_METRICS_KAFKA_SET_INSTANCE_ID_PROPERTY, false);
                LOG.info(TIMELINE_METRICS_KAFKA_INSTANCE_ID_PROPERTY + " = " + instanceId);
                LOG.info(TIMELINE_METRICS_KAFKA_SET_INSTANCE_ID_PROPERTY + " = " + setInstanceId);

                hostInMemoryAggregationEnabled = props.getBoolean(TIMELINE_METRICS_KAFKA_HOST_IN_MEMORY_AGGREGATION_ENABLED_PROPERTY, false);
                hostInMemoryAggregationPort = props.getInt(TIMELINE_METRICS_KAFKA_HOST_IN_MEMORY_AGGREGATION_PORT_PROPERTY, 61888);
                hostInMemoryAggregationProtocol = props.getString(TIMELINE_METRICS_KAFKA_HOST_IN_MEMORY_AGGREGATION_PROTOCOL_PROPERTY, "http");
                setMetricsCache(new TimelineMetricsCache(maxRowCacheSize, metricsSendInterval));
                LOG.info(TIMELINE_METRICS_KAFKA_HOST_IN_MEMORY_AGGREGATION_ENABLED_PROPERTY + " = " + hostInMemoryAggregationEnabled);
                LOG.info(TIMELINE_METRICS_KAFKA_HOST_IN_MEMORY_AGGREGATION_PORT_PROPERTY + " = " + hostInMemoryAggregationPort);
                LOG.info(TIMELINE_METRICS_KAFKA_HOST_IN_MEMORY_AGGREGATION_PROTOCOL_PROPERTY + " = " + hostInMemoryAggregationProtocol);

                if (metricCollectorProtocol.contains("https") || hostInMemoryAggregationProtocol.contains("https")) {
                    String trustStorePath = props.getString(TIMELINE_METRICS_SSL_KEYSTORE_PATH_PROPERTY).trim();
                    String trustStoreType = props.getString(TIMELINE_METRICS_SSL_KEYSTORE_TYPE_PROPERTY).trim();
                    String trustStorePwd = props.getString(TIMELINE_METRICS_SSL_KEYSTORE_PASSWORD_PROPERTY).trim();
                    loadTruststore(trustStorePath, trustStoreType, trustStorePwd);
                }

                // Exclusion policy
                String excludedMetricsStr = props.getString(EXCLUDED_METRICS_PROPERTY, "");
                if (!StringUtils.isEmpty(excludedMetricsStr.trim())) {
                    excludedMetricsPrefixes = excludedMetricsStr.trim().split(",");
                }
                // Inclusion override
                String includedMetricsStr = props.getString(INCLUDED_METRICS_PROPERTY, "");
                if (!StringUtils.isEmpty(includedMetricsStr.trim())) {
                    includedMetricsPrefixes = includedMetricsStr.trim().split(",");
                }
                LOG.info(EXCLUDED_METRICS_PROPERTY + " = " + excludedMetricsPrefixes);
                LOG.info(INCLUDED_METRICS_PROPERTY + " = " + includedMetricsStr);

                initializeReporter();
                if (props.getBoolean(TIMELINE_REPORTER_ENABLED_PROPERTY, false)) {
                    startReporter(metricsConfig.pollingIntervalSecs());
                    LOG.info(TIMELINE_REPORTER_ENABLED_PROPERTY + " = true");
                } else {
                    LOG.info(TIMELINE_REPORTER_ENABLED_PROPERTY + " = false");
                }

                LOG.info("Init props: " + props.props());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("MetricsSendInterval = " + metricsSendInterval);
                    LOG.debug("MaxRowCacheSize = " + maxRowCacheSize);
                    LOG.debug("Excluded metrics prefixes = " + excludedMetricsStr);
                    LOG.debug("Included metrics prefixes = " + includedMetricsStr);
                }
            }
        }
    }

    public String getMBeanName() {
        return "kafka:type=org.apache.hadoop.metrics2.sink.kafka.KafkaclientTimelineMetricsReporter";
    }

    public synchronized void startReporter(long period) {
        synchronized (reporterLock) {
            if (initialized && !running) {
                reporter.start(period, TimeUnit.SECONDS);
                running = true;
                LOG.info(String.format("Started Kafka Client Timeline metrics reporter with polling period %d seconds", period));
            }
        }
    }

    public synchronized void stopReporter() {
        synchronized (reporterLock) {
            if (initialized && running) {
                reporter.stop();
                running = false;
                LOG.info("Stopped Kafka Client Timeline metrics reporter");
                initializeReporter();
            }
        }
    }

    private void initializeReporter() {
        reporter = new KafkaclientTimelineMetricsReporter.TimelineScheduledReporter(metrics, clientId,"kc-timeline-scheduled-reporter",
                TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
        initialized = true;
    }

    protected boolean isExcludedMetric(String metricName) {
        if (excludedMetrics.contains(metricName)) {
            return true;
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("metricName => " + metricName +
                    ", exclude: " + StringUtils.startsWithAny(metricName, excludedMetricsPrefixes) +
                    ", include: " + StringUtils.startsWithAny(metricName, includedMetricsPrefixes));
        }
        if (StringUtils.startsWithAny(metricName, excludedMetricsPrefixes)) {
            if (!StringUtils.startsWithAny(metricName, includedMetricsPrefixes)) {
                excludedMetrics.add(metricName);
                return true;
            }
        }
        return false;
    }

    class TimelineScheduledReporter extends KafkaclientScheduledReporter {

        // Todo. make this configurable
        private String APP_ID; // = "kafka_client";

        protected TimelineScheduledReporter(Map<org.apache.kafka.common.MetricName, KafkaMetric> metrics, String cliendId, String name, TimeUnit rateUnit, TimeUnit durationUnit) {
            super(metrics, name, rateUnit, durationUnit);
            APP_ID = cliendId;
        }

        @Override
        public void report(Set<Entry<org.apache.kafka.common.MetricName, KafkaMetric>> metrics) {
            final List<TimelineMetric> metricsList = new ArrayList<TimelineMetric>();
            try {
                for (Entry<org.apache.kafka.common.MetricName, KafkaMetric> entry : metrics) {
                    final org.apache.kafka.common.MetricName metricName = entry.getKey();
                    final KafkaMetric metric = entry.getValue();
                    processMetric(metricName, metric, metricsList);
                }
            } catch (Throwable t) {
                LOG.error("Exception processing Kafka metric", t);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Metrics List size: " + metricsList.size());
                LOG.debug("Metics Set size: " + metrics.size());
                LOG.debug("Excluded metrics set: " + excludedMetrics);
            }

            if (!metricsList.isEmpty()) {
                TimelineMetrics timelineMetrics = new TimelineMetrics();
                timelineMetrics.setMetrics(metricsList);
                try {
                    emitMetrics(timelineMetrics);
                } catch (Throwable t) {
                    LOG.error("Exception emitting metrics", t);
                }
            }
        }

        private void processMetric(org.apache.kafka.common.MetricName metricName, KafkaMetric metric, List<TimelineMetric> timelineMetricsList) {
            // Skip NaN
            if (! Double.isNaN(metric.value())) {
                final long currentTimeMillis = System.currentTimeMillis();
                final String sanitizedName = sanitizeName(metricName);
                TimelineMetric timelineMetric = cacheSanitizedTimelineMetric(currentTimeMillis, sanitizedName, metric.value());
                timelineMetricsList.add(timelineMetric);
            }
        }

        private TimelineMetric cacheSanitizedTimelineMetric(long currentTimeMillis, String sanitizedName, Number metricValue) {
            final TimelineMetric metric = createTimelineMetric(currentTimeMillis, APP_ID, sanitizedName, metricValue);
            // Skip cache if we decide not to include the metric
            // Cannot do this before calculations of percentiles
            if (!isExcludedMetric(sanitizedName)) {
                metricsCache.putTimelineMetric(metric);
            }
            return metric;
        }

        private TimelineMetric createTimelineMetric(long currentTimeMillis, String component, String attributeName,
                                                    Number attributeValue) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Creating timeline metric: " + attributeName + " = " + attributeValue + " time = "
                        + currentTimeMillis + " app_id = " + component);
            }
            TimelineMetric timelineMetric = new TimelineMetric();
            timelineMetric.setMetricName(attributeName);
            timelineMetric.setHostName(hostname);
            if (setInstanceId) {
                timelineMetric.setInstanceId(instanceId);
            }
            timelineMetric.setAppId(component);
            timelineMetric.setStartTime(currentTimeMillis);
            timelineMetric.setType(ClassUtils.getShortCanonicalName(attributeValue, "Number"));
            timelineMetric.getMetricValues().put(currentTimeMillis, attributeValue.doubleValue());
            return timelineMetric;
        }

        protected String sanitizeName(org.apache.kafka.common.MetricName name) {
            if (name == null) {
                return "";
            }
            final StringBuilder nameBuilder = new StringBuilder();
            nameBuilder.append(name.group()).append(".").append(name.name());
            final List<String> tagKeys = new ArrayList<>(name.tags().keySet());
            // sort according to tags keys to make name consistent
            Collections.sort(tagKeys);
            for (String tagKey: tagKeys) {
                nameBuilder.append(".").append(tagKey).append(".").append(name.tags().get(tagKey));
            }
            final String metricName = nameBuilder.toString();

            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < metricName.length(); i++) {
                final char p = metricName.charAt(i);
                if (!(p >= 'A' && p <= 'Z') && !(p >= 'a' && p <= 'z') && !(p >= '0' && p <= '9') && (p != '_') && (p != '-')
                        && (p != '.') && (p != '\0')) {
                    sb.append('_');
                } else {
                    sb.append(p);
                }
            }
            return sb.toString();
        }
    }

    private final Map<org.apache.kafka.common.MetricName, KafkaMetric> metrics = new LinkedHashMap<>();
    private final Object metricsLock = new Object();

    @Override
    public void configure(Map<String, ?> configs) {
        Properties tempProps = new Properties();
        tempProps.putAll(configs);
        VerifiableProperties props = new VerifiableProperties(tempProps);
        init(props);
    }

    @Override
    public void init(List<KafkaMetric> initMetrics) {
        synchronized (metricsLock) {
            for (KafkaMetric metric : initMetrics) {
                LOG.debug("Adding metric " + metric.metricName());
                metrics.put(metric.metricName(), metric);
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (metricsLock) {
            LOG.debug("Updating metric " + metric.metricName());
            metrics.put(metric.metricName(), metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (metricsLock) {
            LOG.debug("Removing metric " + metric.metricName());
            metrics.remove(metric.metricName());
        }
    }

    @Override
    public void close() {
        stopReporter();
    }

}
