package com.didichuxing.datachannel.arius.fastindex.mr.embeddedes;

public class IndexRequest {

    private final String indexName;
    private final String indexType;
    private final String id;
    private final String routing;
    private final String json;

    private IndexRequest(String indexName, String indexType, String json, String id, String routing) {
        this.indexName = indexName;
        this.indexType = indexType;
        this.id = id;
        this.routing = routing;
        this.json = json;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getId() {
        return id;
    }

    public String getRouting() {
        return routing;
    }

    public String getJson() {
        return json;
    }

    public static class IndexRequestBuilder {

        private String indexName;
        private String indexType;
        private String id;
        private String routing;
        private String json;

        public IndexRequestBuilder(final String indexName, final String indexType, final String json) {
            this.indexName = indexName;
            this.indexType = indexType;
            this.json = json;
        }

        public IndexRequestBuilder withIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public IndexRequestBuilder withIndexType(String indexType) {
            this.indexType = indexType;
            return this;
        }

        public IndexRequestBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public IndexRequestBuilder withRouting(String routing) {
            this.routing = routing;
            return this;
        }

        public IndexRequestBuilder withJson(String json) {
            this.json = json;
            return this;
        }

        public IndexRequest build() {
            return new IndexRequest(indexName, indexType, json, id, routing);
        }
    }
}
