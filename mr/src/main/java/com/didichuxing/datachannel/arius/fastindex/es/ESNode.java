package com.didichuxing.datachannel.arius.fastindex.es;

import com.alibaba.fastjson.JSONObject;
import com.didichuxing.datachannel.arius.fastindex.embeddedes.EmbeddedElastic;
import com.didichuxing.datachannel.arius.fastindex.embeddedes.PopularProperties;
import com.didichuxing.datachannel.arius.fastindex.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.remote.config.TaskConfig;
import com.didichuxing.datachannel.arius.fastindex.utils.HdfsUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.didichuxing.datachannel.arius.fastindex.utils.PortUtils.choicePort;

@Slf4j
public class ESNode {
    private static final String ELASTICSEARCH_6_6_1_ZIP_STR = "elasticsearch-6.6.1.zip";
    public static final String LOCAL_ES_WORK_PATH = "ariusFastIndex";
    private static final String LOCAL_CLUSTER_NAME = "fast_index_cluster";
    private static final String LOCAL_INDEX_NAME = "fast_index";


    @Getter
    private TaskConfig taskConfig;

    @Getter
    private IndexInfo indexInfo;

    @Getter
    private EmbeddedElastic elastic;

    @Getter
    private ESClient esClient;

    private String localWorkPath;

    public ESNode(TaskConfig taskConfig, IndexInfo indexInfo) {
        this.taskConfig = taskConfig;
        this.indexInfo = indexInfo;

        File file = new File(LOCAL_ES_WORK_PATH);
        this.localWorkPath = file.getAbsolutePath();
    }

    /* 初始化es容器 */
    public void init(Reducer.Context context) throws Exception {
        // 清理es工作目录
        HdfsUtil.deleteDir(context.getConfiguration(), localWorkPath);
        // 启动es进程
        start();
    }

    /* 启动容器 */
    public void start() throws Exception {
        List<Integer> ports = choicePort();

        elastic = EmbeddedElastic.builder()
                .withSetting(PopularProperties.HTTP_PORT, ports.get(0))
                .withSetting(PopularProperties.TRANSPORT_TCP_PORT, ports.get(1))
                .withSetting(PopularProperties.CLUSTER_NAME, LOCAL_CLUSTER_NAME)
                .withSetting("discovery.type", "single-node")
                .withSetting(PopularProperties.PATH_DATA, getDataDir())
                .withInstallationDirectory(new File(localWorkPath + "/es"))
                .withDownloadDirectory(new File(localWorkPath + "/download"))
                .withInResourceLocation(ELASTICSEARCH_6_6_1_ZIP_STR)
                .withStartTimeout(120, TimeUnit.SECONDS)
                .build().start();

        esClient = new ESClient(elastic.getHttpPort(), LOCAL_INDEX_NAME, indexInfo.getType());

        // 配置集群属性
        JSONObject param = new JSONObject();
        param.put("cluster.routing.allocation.disk.watermark.flood_stage", "10mb");
        param.put("cluster.routing.allocation.disk.watermark.high", "20mb");
        param.put("cluster.routing.allocation.disk.watermark.low", "30mb");
        esClient.putClusterSetting(param);

        // 创建新的index
        esClient.createNewIndex(indexInfo.getSetting());
    }



    public void stop() {
        log.info("close es node start");
        try {
            if (this.elastic!= null) {
                elastic.stop();
            }
        } catch (Throwable t) {
            log.info("close es error", t);
        }
        log.info("close es node stop");
    }

    public String getDataDir() {
        return localWorkPath + "/data";
    }
}
