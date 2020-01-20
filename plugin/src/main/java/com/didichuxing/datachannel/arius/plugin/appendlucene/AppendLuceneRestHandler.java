package com.didichuxing.datachannel.arius.plugin.appendlucene;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestBuilderListener;


/* 增加http接口，调用方式 curl "127.0.0.1:9200/lucene/append?indexName=$indexName&uuid=$uuid&shardId=$indexShard&append=$dir&primeKey=$primeKey" */
public class AppendLuceneRestHandler extends BaseRestHandler {

    public AppendLuceneRestHandler(Settings settings, RestController restController) {
        super(settings);
        restController.registerHandler(RestRequest.Method.POST, "/lucene/append", this);
        restController.registerHandler(RestRequest.Method.GET, "/lucene/append", this);
    }

    @Override
    public String getName() {
        return "append-lucene";
    }

    private static final String INDEX_NAME  = "indexName";
    private static final String UUID        = "uuid";
    private static final String SHARD_ID    = "shardId";
    private static final String APPEND      = "append";
    private static final String PRIMERKEY = "primeKey";


    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        AppendLuceneRequest appendLuceneRequest = new AppendLuceneRequest();

        appendLuceneRequest.indexName = request.param(INDEX_NAME);
        appendLuceneRequest.uuid = request.param(UUID);
        appendLuceneRequest.shardId = Integer.valueOf(request.param(SHARD_ID));
        appendLuceneRequest.appendSegmentDirs = request.param(APPEND);
        appendLuceneRequest.primeKey = request.param(PRIMERKEY);

        // 跳转到AppendLuceneTransportAction.doExecute()
        return channel -> client.executeLocally(AppendLuceneAction.INSTANCE, appendLuceneRequest, new RestBuilderListener<AppendLuceneResponse>(channel) {
            @Override
            public RestResponse buildResponse(AppendLuceneResponse appendLuceneResponse, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, appendLuceneResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }
        });
    }
}
