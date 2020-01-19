package com.didichuxing.datachannel.arius.plugin.appendlucene;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class AppendLuceneResponse extends ActionResponse implements ToXContentFragment {
    public long deleteCount;

    public AppendLuceneResponse() {}

    private static final String APPEND_LUCENE_OK = "APPEND_LUCENE_OK";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("msg", APPEND_LUCENE_OK);
        builder.field("delete_count", deleteCount);
        builder.endObject();
        return builder;
    }
}
