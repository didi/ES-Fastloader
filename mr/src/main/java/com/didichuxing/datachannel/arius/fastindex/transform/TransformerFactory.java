package com.didichuxing.datachannel.arius.fastindex.transform;

import com.didichuxing.datachannel.arius.fastindex.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.remote.config.TaskConfig;
import com.didichuxing.datachannel.arius.fastindex.transform.normal.NormalTransformer;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;
import org.apache.hadoop.mapreduce.Reducer;

public class TransformerFactory {
    public static Transformer getTransoformer(Reducer.Context context, TaskConfig taskConfig, IndexInfo indexInfo) throws Exception {
        // 默认使用normal转化
        LogUtils.info("use transormer name:default");
        return new NormalTransformer(context, indexInfo);
    }
}
