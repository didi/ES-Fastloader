package com.didichuxing.fastindex.job;


import com.didichuxing.fastindex.common.es.TemplateConfig;
import com.didichuxing.fastindex.common.po.FastIndexOpIndexPo;
import com.didichuxing.fastindex.dao.FastIndexOpIndexDao;
import com.didichuxing.fastindex.dao.FastIndexTemplateConfigDao;
import com.didichuxing.fastindex.dao.IndexOpDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;


// 负责处理单个索引
//@JobHandler(value = "fastIndexOpIndexCollector")
@Slf4j
@Component
public class FastIndexOpIndexCollector {

    @Autowired
    private FastIndexOpIndexDao fastIndexOpIndexDao;

    @Autowired
    private FastIndexTemplateConfigDao fastIndexTemplateConfigDao;

    @Autowired
    private IndexOpDao indexOpDao;

    public void handleJobTask(String params) {
        log.info("class=FastIndexCleanDataCollector ||method=handleJobTask||params={}", params);
        List<FastIndexOpIndexPo> pos = fastIndexOpIndexDao.getUnFinished();
        if(pos==null || pos.size()==0) {
            return ;
        }

        log.info("fastIndexOpIndexCollector get poList size:" + pos.size());

        for(FastIndexOpIndexPo po : pos) {
            String template = po.getTemplateName();
            String index = po.getIndexName();

            try {

                // 打开索引可写标记
                indexOpDao.updateSetting(index, "blocks.write", "false");

                // 将索引配置成可以rebalance
                indexOpDao.updateSetting(index, "routing.rebalance.enable", "all");

                // 根据模板配置，配置对应副本数
                List<String> templates = new ArrayList<>();
                templates.add(template);

                TemplateConfig templateConfig = indexOpDao.getTemplate(template);

                if(templateConfig==null) {
                    log.warn("fastIndexOpIndexCollector get null templateConfig, template:" + template);
                } else {
                    String value = templateConfig.getSetttings().get("number_of_replicas");
                    if(value!=null) {
                        indexOpDao.updateSetting(index, "number_of_replicas", value);
                    }
                }

                po.setFinish(true);
                po.setFinishTime(System.currentTimeMillis());
            } catch (Throwable t) {
                log.warn("fastIndexOpIndexCollector error, index:" + index, t);
            }
        }

        fastIndexOpIndexDao.batchInsert(pos);
    }
}
