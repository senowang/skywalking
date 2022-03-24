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

import org.apache.skywalking.oap.server.core.analysis.management.ManagementData;
import org.apache.skywalking.oap.server.core.storage.StorageHashMapBuilder;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.IndexController;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.ManagementEsDAO;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util.DataWrapper;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util.KafkaSenderHandler;

import java.io.IOException;
import java.util.Map;

public class KafkaEsManagementEsDAO extends ManagementEsDAO {
    private final StorageHashMapBuilder<ManagementData> storageBuilder;

    public KafkaEsManagementEsDAO(ElasticSearchClient client,
                                  StorageHashMapBuilder<ManagementData> storageBuilder) {
        super(client, storageBuilder);
        this.storageBuilder = storageBuilder;
    }

    @Override
    public void insert(Model model, ManagementData managementData) throws IOException {
        String tableName = IndexController.INSTANCE.getTableName(model);
        String docId = IndexController.INSTANCE.generateDocId(model, managementData.id());
        final boolean exist = getClient().existDoc(tableName, docId);
        if (exist) {
            return;
        }
        Map<String, Object> source =
                IndexController.INSTANCE.appendMetricTableColumn(model, storageBuilder.entity2Storage(
                        managementData));
        //sender to kafka
        super.insert(model, managementData);
        KafkaSenderHandler.getInstance().sender(new DataWrapper(source, IndexController.INSTANCE.getTableName(model), false, docId));
    }
}
