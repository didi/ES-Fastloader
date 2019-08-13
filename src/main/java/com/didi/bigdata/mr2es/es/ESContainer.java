package com.didi.bigdata.mr2es.es;

import com.alibaba.fastjson.JSONObject;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.didi.bigdata.mr2es.utils.HiveESFieldMappingUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * es 容器
 * Created by WangZhuang on 2018/12/5
 */
@Slf4j
public class ESContainer {

    @Getter
    private Node node;

    public void start(String workDir, String clusterName) {
        Settings settings = Settings.settingsBuilder()
                .put("http.enabled", false)
                .put("node.name", "es_node1")
                .put("path.home", workDir)
                .put("path.data", workDir)
                .put("Dlog4j2.enable.threadlocals", false)
                .build();
        this.node = NodeBuilder.nodeBuilder()
                .client(false)
                .local(true)
                .data(true)
                .clusterName(clusterName)
                .settings(settings)
                .build();
        this.node.start();
    }

    public void createNewIndex(String indexName, String source) {
        node.client().admin().indices()
                .prepareCreate(indexName).setSource(source).get();
    }

    public void createIndex(String indexName, int shards, int replicas) {
        Settings indexSettings;
        indexSettings = Settings.settingsBuilder()
                .put("number_of_shards", shards)
                .put("number_of_replicas", replicas)
                .build();

        CreateIndexRequest createIndexRequest =
                new CreateIndexRequest(indexName, indexSettings);
        node.client().admin().indices()
                .create(createIndexRequest).actionGet();
    }

    public void createIndexMapping(String indexName, String indexType,
                                   List<HCatFieldSchema> fields) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_all").field("enabled", "true").endObject()
                .startObject("_source").field("enabled", "true").endObject();
        mapping.startObject("properties");

        for (HCatFieldSchema field : fields) {
            String fieldName = field.getName();
            String fieldType = HiveESFieldMappingUtil.getInstance()
                    .getESFieldsMapping(field.getTypeString());
            if (fieldType.equals("string")) {
                mapping.startObject(fieldName)
                        .field("type", fieldType)
                        .field("index", "not_analyzed");
                mapping.endObject();
            } else {
                mapping.startObject(fieldName).field("type", fieldType);
                mapping.endObject();
            }
        }
        mapping.endObject();
        mapping.endObject();
        PutMappingRequest putMappingRequest =
                new PutMappingRequest(indexName).type(indexType).source(mapping);
        node.client().admin().indices().putMapping(putMappingRequest).actionGet();
    }

    /**
     * index是否存在
     *
     * @param indexName
     * @return
     */
    public boolean indexIsExist(String indexName) {
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = node.client().admin().indices().exists(request).actionGet();
        return response.isExists();
    }

    public JSONObject getMapping(String index, String type) throws IOException {
        GetMappingsRequestBuilder builder = node.client()
                .admin().indices().prepareGetMappings(index).addTypes(type);
        GetMappingsResponse response = builder.execute().actionGet();
        for(ObjectCursor<MappingMetaData> mapping :
                response.getMappings().get(index).values()) {
            Map<String, Object> map = mapping.value.getSourceAsMap();
            if (MapUtils.isNotEmpty(map)) {
                return new JSONObject(map);
            }
        }
        return null;
    }

    /**
     * index是否存在
     *
     * @param indexName
     * @return
     */
    public boolean forceMerge(String indexName) {
        ForceMergeResponse response = node.client().admin()
                .indices().prepareForceMerge(indexName).setMaxNumSegments(1)
                .get();
        return true;
    }

    public void closeNode() {
        if (this.node != null) {
            this.node.close();
        }
    }

}
