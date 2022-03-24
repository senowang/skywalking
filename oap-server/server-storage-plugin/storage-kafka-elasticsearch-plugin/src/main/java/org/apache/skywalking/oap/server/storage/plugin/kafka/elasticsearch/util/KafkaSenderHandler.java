package org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.skywalking.oap.server.library.module.ModuleStartException;
import org.apache.skywalking.oap.server.library.server.pool.CustomThreadFactory;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.StorageModuleKafkaElasticsearchConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class KafkaSenderHandler implements Runnable {
    private static final Gson GSON = new Gson();
    private static KafkaSenderHandler INSTANCE;
    private final KafkaProducer<String, String> kafkaProducer;
    private final Properties properties;
    private final StorageModuleKafkaElasticsearchConfig config;
    private final ThreadPoolExecutor executor;
    private final ArrayBlockingQueue<DataWrapper> arrayBlockingQueue;
    private int dataCacheSize = 10000;

    public KafkaSenderHandler(StorageModuleKafkaElasticsearchConfig config) {
        this.config = config;
        properties = new Properties();
        properties.putAll(config.getKafkaProducerConfig());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        kafkaProducer = new KafkaProducer<>(properties);
        if (config.getDataCacheSize() > 0) {
            dataCacheSize = config.getDataCacheSize();
        }
        arrayBlockingQueue = new ArrayBlockingQueue<>(dataCacheSize);
        executor = new ThreadPoolExecutor(1, 1,
                60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new CustomThreadFactory("KafkaSender"),
                new ThreadPoolExecutor.CallerRunsPolicy());
        INSTANCE = this;
    }

    public static KafkaSenderHandler getInstance() {
        return INSTANCE;
    }

    public void sender(DataWrapper dataWrapper) {
        if (config.isEnableKafka()) {
            if (dataWrapper != null) {
                boolean flag = arrayBlockingQueue.offer(dataWrapper);

                if (!flag) {
                    int tryCount = 0;
                    do {
                        flag = arrayBlockingQueue.offer(dataWrapper);
                        if (flag) {
                            break;
                        } else {
                            log.warn("add data to cache failed, retry time {}", tryCount);
                        }
                        tryCount--;
                    } while (tryCount <= 3);
                    if (!flag) {
                        log.error("add data to cache failed");
                    }
                }
            }
        }
    }

    public void start() throws ModuleStartException {
        createTopicIfNeeded();
        executor.submit(this);
    }

    @Override
    public void run() {
        while (true) {
            try {
                DataWrapper take = arrayBlockingQueue.take();
                String topic = config.getTopicName();
                String key = take.getId();

                if (config.isEnableDataShare()) {
                    topic = config.getTopicName();
                } else {
                    String moduleName = take.getModuleName();
                    if (moduleName.startsWith("metric")) {
                        topic = config.getMetricTopicName();
                        take.setDataSourceCode(config.getMetricDataTagCode());
                    } else if (moduleName.startsWith("segment")) {
                        topic = config.getSegmentTopicName();
                        take.setDataSourceCode(config.getSegmentDataTagCode());
                    } else if (moduleName.startsWith("alarm")) {
                        topic = config.getAlarmTopicName();
                        take.setDataSourceCode(config.getAlarmDataTagCode());
                    } else if (moduleName.endsWith("_side") || moduleName.endsWith("network_address_alias")) {
                        topic = config.getTopologyTopicName();
                        take.setDataSourceCode(config.getTopologyDataTagCode());
                    } else {
                        topic = config.getOtherTopicName();
                        take.setDataSourceCode(config.getOtherDataTagCode());
                    }
                }
                String data = GSON.toJson(take);
                kafkaProducer.send(new ProducerRecord<>(topic, key, data));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error(e.getMessage());
            }
        }
    }

    public void createTopicIfNeeded() throws ModuleStartException {

        boolean enableDataShare = config.isEnableDataShare();
        List<String> topics = null;
        if (enableDataShare) {
            topics = Collections.singletonList(config.getTopicName());
        } else {
            topics = new ArrayList<>();
            Collections.addAll(topics, config.getTopologyTopicName(), config.getAlarmTopicName(),
                    config.getSegmentTopicName(), config.getMetricTopicName(), config.getOtherTopicName());
        }
        createTopicIfNeeded(topics, properties);
    }

    private void createTopicIfNeeded(Collection<String> topics, Properties properties) throws ModuleStartException {
        AdminClient adminClient = AdminClient.create(properties);
        Set<String> missedTopics = adminClient.describeTopics(topics)
                .values()
                .entrySet()
                .stream()
                .map(entry -> {
                    try {
                        entry.getValue().get();
                        return null;
                    } catch (InterruptedException | ExecutionException ignore) {
                    }
                    return entry.getKey();
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        if (!missedTopics.isEmpty()) {
            log.info("Topics" + missedTopics.toString() + " not exist.");
            List<NewTopic> newTopicList = missedTopics.stream()
                    .map(topic -> new NewTopic(
                            topic,
                            config.getPartitions(),
                            (short) config.getReplicationFactor()
                    )).collect(Collectors.toList());

            try {
                adminClient.createTopics(newTopicList).all().get();
            } catch (Exception e) {
                throw new ModuleStartException("Failed to create Kafka Topics" + missedTopics + ".", e);
            }
        }
    }
}
