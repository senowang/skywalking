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

import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.storage.StorageHashMapBuilder;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.library.client.elasticsearch.IndexRequestWrapper;
import org.apache.skywalking.oap.server.library.client.elasticsearch.UpdateRequestWrapper;
import org.apache.skywalking.oap.server.library.client.request.InsertRequest;
import org.apache.skywalking.oap.server.library.client.request.UpdateRequest;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.IndexController;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.MetricsEsDAO;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util.DataWrapper;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util.KafkaSenderHandler;

import java.util.Map;

@Slf4j
public class KafkaEsMetricsEsDAO extends MetricsEsDAO {

    protected KafkaEsMetricsEsDAO(ElasticSearchClient client,
                                  StorageHashMapBuilder<Metrics> storageBuilder) {
        super(client, storageBuilder);
    }

    @Override
    public InsertRequest prepareBatchInsert(Model model, Metrics metrics) {
        InsertRequest insertRequest = super.prepareBatchInsert(model, metrics);
        IndexRequestWrapper indexRequestWrapper = (IndexRequestWrapper) insertRequest;
        Map<String, ?> doc = indexRequestWrapper.getRequest().getDoc();
        String id = indexRequestWrapper.getRequest().getId();
        //sender to kafka
        KafkaSenderHandler.getInstance().sender(new DataWrapper(doc, IndexController.INSTANCE.getTableName(model), false, id));
        return insertRequest;
    }

    @Override
    public UpdateRequest prepareBatchUpdate(Model model, Metrics metrics) {
        UpdateRequest updateRequest = super.prepareBatchUpdate(model, metrics);
        UpdateRequestWrapper updateRequestWrapper = (UpdateRequestWrapper) updateRequest;
        Map<String, ?> doc = updateRequestWrapper.getRequest().getDoc();
        String id = updateRequestWrapper.getRequest().getId();
        //sender to kafka
        KafkaSenderHandler.getInstance().sender(new DataWrapper(doc, IndexController.INSTANCE.getTableName(model), true, id));
        return updateRequest;
    }
}
