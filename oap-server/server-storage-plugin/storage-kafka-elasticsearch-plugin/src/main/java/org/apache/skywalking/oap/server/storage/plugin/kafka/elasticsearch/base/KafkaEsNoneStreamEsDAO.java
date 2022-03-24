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

package org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.base;

import org.apache.skywalking.oap.server.core.analysis.config.NoneStream;
import org.apache.skywalking.oap.server.core.storage.StorageHashMapBuilder;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.IndexController;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.NoneStreamEsDAO;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util.DataWrapper;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util.KafkaSenderHandler;

import java.io.IOException;
import java.util.Map;

/**
 * Synchronize storage Elasticsearch implements
 */
public class KafkaEsNoneStreamEsDAO extends NoneStreamEsDAO {
    private final StorageHashMapBuilder<NoneStream> storageBuilder;

    public KafkaEsNoneStreamEsDAO(ElasticSearchClient client,
                                  StorageHashMapBuilder<NoneStream> storageBuilder) {
        super(client, storageBuilder);
        this.storageBuilder = storageBuilder;
    }

    @Override
    public void insert(Model model, NoneStream noneStream) throws IOException {
        Map<String, Object> builder =
                IndexController.INSTANCE.appendMetricTableColumn(model, storageBuilder.entity2Storage(
                        noneStream));
        String id = IndexController.INSTANCE.generateDocId(model, noneStream.id());
        //sender to kafka
        KafkaSenderHandler.getInstance().sender(new DataWrapper(builder, IndexController.INSTANCE.getTableName(model), false, id));
        super.insert(model, noneStream);
    }
}
