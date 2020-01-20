package com.didichuxing.fastindex.common.po;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.UnsupportedEncodingException;
import java.util.Base64;


/* 对应单个reducer任务产生的mapping */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FastIndexMappingPo extends BasePo {
    private String srcTag;
    private String indexName;   // 索引名
    private long reduceId;      // reduce编号
    private String mappingMd5;  // mapping数据
    private long addTime;       // 提交时间

    @JSONField(serialize = false)
    public void setMapping(String mapping) throws UnsupportedEncodingException {
        this.mappingMd5 = Base64.getEncoder().encodeToString(mapping.toString().getBytes("UTF-8"));;
    }

    @JSONField(serialize = false)
    public String getMapping() throws UnsupportedEncodingException {
        return new String(Base64.getDecoder().decode(mappingMd5), "UTF-8");
    }

    @Override
    public String getKey() {
        if(srcTag==null || srcTag.trim().length()==0) {
            return indexName + "_" + reduceId;
        } else {
            return srcTag.trim() + "_" + indexName + "_" + reduceId;
        }
    }
}
