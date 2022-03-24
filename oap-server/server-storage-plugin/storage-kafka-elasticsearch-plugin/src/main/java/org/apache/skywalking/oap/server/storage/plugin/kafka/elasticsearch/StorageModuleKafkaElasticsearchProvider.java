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

import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.storage.StorageDAO;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.library.module.ModuleConfig;
import org.apache.skywalking.oap.server.library.module.ModuleDefine;
import org.apache.skywalking.oap.server.library.module.ModuleStartException;
import org.apache.skywalking.oap.server.library.module.ServiceNotProvidedException;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.StorageModuleElasticsearchProvider;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.base.KafkaEsStorageEsDao;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util.KafkaSenderHandler;

/**
 * The storage provider for kafka and ElasticSearch
 */
@Slf4j
public class StorageModuleKafkaElasticsearchProvider extends StorageModuleElasticsearchProvider {

    protected final StorageModuleKafkaElasticsearchConfig config;

    public StorageModuleKafkaElasticsearchProvider() {
        super();
        this.config = new StorageModuleKafkaElasticsearchConfig();
    }

    @Override
    public String name() {
        return "kafka-elasticsearch";
    }

    @Override
    public Class<? extends ModuleDefine> module() {
        return StorageModule.class;
    }

    @Override
    public ModuleConfig createConfigBeanIfAbsent() {
        return config;
    }

    @Override
    public void prepare() throws ServiceNotProvidedException {
        super.prepare();
        this.registerServiceImplementation(StorageDAO.class, new KafkaEsStorageEsDao(elasticSearchClient));
    }

    @Override
    public void start() throws ModuleStartException {
        super.start();
        if (config.isEnableKafka()) {
            KafkaSenderHandler kafkaSenderHandler = new KafkaSenderHandler(config);
            kafkaSenderHandler.start();
        }

    }

}
