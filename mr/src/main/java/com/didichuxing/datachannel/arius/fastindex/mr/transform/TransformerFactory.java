package com.didichuxing.datachannel.arius.fastindex.mr.transform;

import com.didichuxing.datachannel.arius.fastindex.mr.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.mr.remote.config.TaskConfig;
import com.didichuxing.datachannel.arius.fastindex.mr.transform.normal.NormalTransformer;
import com.didichuxing.datachannel.arius.fastindex.mr.utils.LogUtils;
import org.apache.hadoop.mapreduce.Reducer;

public class TransformerFactory {
    public static Transformer getTransoformer(Reducer.Context context, TaskConfig taskConfig, IndexInfo indexInfo) throws Exception {
        // 默认使用normal转化
        LogUtils.info("use transormer name:default");
        return new NormalTransformer(context, indexInfo);
    }
}
