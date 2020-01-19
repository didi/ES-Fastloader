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

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class AppendLuceneTransportAction extends TransportAction<AppendLuceneRequest, AppendLuceneResponse> {

    private ThreadPool threadPool;
    private IndicesService indicesService;

    @Inject
    public AppendLuceneTransportAction(Settings settings,
                                       ThreadPool threadPool,
                                       ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       TransportService transportService,
                                       IndicesService indexServices) {

        super(settings, AppendLuceneAction.NAME, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.threadPool = threadPool;
        this.indicesService = indexServices;
    }

    @Override
    protected void doExecute(AppendLuceneRequest request, ActionListener<AppendLuceneResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).submit(() -> {
            doExecuteCore(request, listener);
        });
    }

    private void doExecuteCore(AppendLuceneRequest request, ActionListener<AppendLuceneResponse> listener) {
        try {
            request.check();

            ShardId shardId = new ShardId(request.indexName, request.uuid, request.shardId);
            IndexShard shard = indicesService.getShardOrNull(shardId);
            if(shard==null) {
                throw new Exception("shard not found, indexName:" + request.indexName + ", shardId:" + request.shardId);
            }

            /* FIXME 这里需要修改es的代码, 将lucene的IndexWriter对象暴露给plugin使用  */
            InternalEngine engine = (InternalEngine) shard.getEngineOrNull();
            IndexWriter indexWriter = engine.getIndexWriter();

            long deleteCount = -1;
            List<String> appendDirs = request.getAppendDirs();
            if(request.primeKey!=null && request.primeKey.length()>0) {
                deleteCount = doPrimerKey(appendDirs, indexWriter, request.primeKey);
            }

            Directory[] indexes = new Directory[appendDirs.size()];
            for(int i=0; i<appendDirs.size(); i++) {
                indexes[i] = FSDirectory.open(Paths.get(appendDirs.get(i)));
            }
            indexWriter.addIndexes(indexes);
            indexWriter.commit();

            AppendLuceneResponse response = new AppendLuceneResponse();
            response.deleteCount = deleteCount;
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }


    public long doPrimerKey(List<String> dirs, IndexWriter dstIndexWriter, String primeKey) throws IOException {
        if (dstIndexWriter.numDocs() == 0) {
            return -1;
        }

        long count = 0;
        for(String appendDir : dirs) {
            IndexWriter srcIndexWriter = null;
            StandardDirectoryReader srcReader = null;
            StandardDirectoryReader dstReader = null;
            try {
                IndexWriterConfig indexWriterConfig = new IndexWriterConfig(null);
                indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
                indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
                srcIndexWriter = new IndexWriter(FSDirectory.open(Paths.get(appendDir)), indexWriterConfig);

                srcReader = (StandardDirectoryReader) DirectoryReader.open(srcIndexWriter);
                dstReader = (StandardDirectoryReader) DirectoryReader.open(dstIndexWriter);


                for (LeafReaderContext srcLeafReaderContext : srcReader.leaves()) {
                    LeafReader srcLeafReader = srcLeafReaderContext.reader();

                    Terms srcTerms = srcLeafReader.terms(primeKey);
                    if (srcTerms == null) {
                        continue;
                    }
                    TermsEnum srcTermsEnum = srcTerms.iterator();

                    while (srcTermsEnum.next() != null) {
                        Term srcTerm = new Term(primeKey, srcTermsEnum.term());

                        // 判断dst中是否存在
                        for (LeafReaderContext dstleafReaderContext : dstReader.leaves()) {
                            LeafReader dstLeafReader = dstleafReaderContext.reader();
                            if (dstLeafReader.postings(srcTerm) != null) {
                                dstIndexWriter.deleteDocuments(srcTerm);
                                count++;
                                break;
                            }
                        }
                    }
                }

            } finally {
                if (srcReader != null) {
                    srcReader.close();
                }

                if (dstReader != null) {
                    dstReader.close();
                }

                if (srcIndexWriter != null) {
                    srcIndexWriter.close();
                }
            }
        }

        return count;
    }
}
