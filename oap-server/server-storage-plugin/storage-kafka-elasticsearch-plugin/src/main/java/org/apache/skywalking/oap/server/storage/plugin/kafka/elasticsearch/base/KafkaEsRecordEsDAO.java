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

import org.apache.skywalking.oap.server.core.analysis.record.Record;
import org.apache.skywalking.oap.server.core.storage.StorageHashMapBuilder;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.library.client.elasticsearch.IndexRequestWrapper;
import org.apache.skywalking.oap.server.library.client.request.InsertRequest;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.IndexController;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.RecordEsDAO;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util.DataWrapper;
import org.apache.skywalking.oap.server.storage.plugin.kafka.elasticsearch.util.KafkaSenderHandler;

import java.io.IOException;
import java.util.Map;

public class KafkaEsRecordEsDAO extends RecordEsDAO {

    public KafkaEsRecordEsDAO(ElasticSearchClient client,
                              StorageHashMapBuilder<Record> storageBuilder) {
        super(client, storageBuilder);
    }

    @Override
    public InsertRequest prepareBatchInsert(Model model, Record record) throws IOException {
        InsertRequest insertRequest = super.prepareBatchInsert(model, record);
        IndexRequestWrapper indexRequestWrapper = (IndexRequestWrapper) insertRequest;
        Map<String, ?> doc = indexRequestWrapper.getRequest().getDoc();
        String id = indexRequestWrapper.getRequest().getId();
        //sender to kafka
        KafkaSenderHandler.getInstance().sender(new DataWrapper(doc, IndexController.INSTANCE.getTableName(model), false, id));
        return insertRequest;
    }
}
