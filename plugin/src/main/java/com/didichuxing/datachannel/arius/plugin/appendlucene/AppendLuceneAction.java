/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.didichuxing.datachannel.arius.plugin.appendlucene;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class AppendLuceneAction extends Action<AppendLuceneRequest, AppendLuceneResponse, AppendLuceneRequestBuilder> {
    public static final AppendLuceneAction INSTANCE = new AppendLuceneAction();
    public static final String NAME = "indices:append/lucene";

    private AppendLuceneAction() {
        super(NAME);
    }

    @Override
    public AppendLuceneRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new AppendLuceneRequestBuilder(client, this);
    }

    @Override
    public AppendLuceneResponse newResponse() {
        return new AppendLuceneResponse();
    }
}
