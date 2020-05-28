package com.didichuxing.datachannel.arius.fastindex.server.job;


import com.didichuxing.datachannel.arius.fastindex.server.common.es.TemplateConfig;
import com.didichuxing.datachannel.arius.fastindex.server.common.po.FastIndexOpIndexPo;
import com.didichuxing.datachannel.arius.fastindex.server.dao.FastIndexOpIndexDao;
import com.didichuxing.datachannel.arius.fastindex.server.dao.IndexOpDao;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;


// 负责处理单个索引
@Slf4j
public class FastIndexOpIndexCollector {
    public static void handleJobTask(String params) {
        log.info("class=FastIndexCleanDataCollector ||method=handleJobTask||params={}", params);
        List<FastIndexOpIndexPo> pos = FastIndexOpIndexDao.getUnFinished();
        if(pos==null || pos.size()==0) {
            return ;
        }

        log.info("fastIndexOpIndexCollector get poList size:" + pos.size());

        for(FastIndexOpIndexPo po : pos) {
            String template = po.getTemplateName();
            String index = po.getIndexName();

            try {

                // 打开索引可写标记
                IndexOpDao.updateSetting(index, "blocks.write", "false");

                // 将索引配置成可以rebalance
                IndexOpDao.updateSetting(index, "routing.rebalance.enable", "all");

                // 根据模板配置，配置对应副本数
                List<String> templates = new ArrayList<>();
                templates.add(template);

                TemplateConfig templateConfig = IndexOpDao.getTemplate(template);

                if(templateConfig==null) {
                    log.warn("fastIndexOpIndexCollector get null templateConfig, template:" + template);
                } else {
                    String value = templateConfig.getSetttings().get("number_of_replicas");
                    if(value!=null) {
                        IndexOpDao.updateSetting(index, "number_of_replicas", value);
                    }
                }

                po.setFinish(true);
                po.setFinishTime(System.currentTimeMillis());
            } catch (Throwable t) {
                log.warn("fastIndexOpIndexCollector error, index:" + index, t);
            }
        }

        FastIndexOpIndexDao.batchInsert(pos);
    }
}
