/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch;

import lombok.Getter;
import lombok.Setter;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.StorageModuleElasticsearchConfig;

import java.util.Properties;

@Getter
@Setter
public class StorageModuleKafkaElasticsearchConfig extends StorageModuleElasticsearchConfig {
    /**
     * Kafka producer config.
     */
    private Properties kafkaProducerConfig = new Properties();

    /**
     * <B>bootstrap.servers</B>: A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
     * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
     */
    private String bootstrapServers;

    /**
     * If true, create the Kafka topic when it does not exist.
     */
    private boolean createTopicIfNotExist = true;

    /**
     * The number of partitions for the topic being created.
     */
    private int partitions = 3;

    /**
     * The replication factor for each partition in the topic being created.
     */
    private int replicationFactor = 2;

    private String topicName = "skywalking-data";
    /**
     * metric
     */
    private String metricTopicName = "skywalking-data-metric";
    /**
     * alarm
     */
    private String alarmTopicName = "skywalking-data-alarm";
    /**
     * segment
     */
    private String segmentTopicName = "skywalking-data-segment";
    /**
     * topology
     */
    private String topologyTopicName = "skywalking-data-topology";
    /**
     * other
     */
    private String otherTopicName = "skywalking-data-other";

    private String segmentDataTagCode = "skywalking_trace";

    private String alarmDataTagCode = "skywalking_alarm";

    private String metricDataTagCode = "skywalking_metric";

    private String topologyDataTagCode = "skywalking_topology";

    private String otherDataTagCode = "skywalking_other";

    private int dataCacheSize;

    private boolean enableKafka = true;

    private boolean enableEs = true;
    /**
     * enable all data to sender one topic
     */
    private boolean enableDataShare = true;

}
