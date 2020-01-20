package com.didichuxing.fastindex.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.didichuxing.fastindex.server.FastIndexService;
import com.didichuxing.fastindex.server.IndexFastIndexInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController()
@RequestMapping("/fastindex")
public class FastIndexController {
    private static final String TEMPLATE_STR    = "template";
    private static final String TIME_STR        = "time";
    private static final String REDUCE_ID_STR   = "reduceId";
    private static final String MAPPING_STR     = "mapping";
    private static final String METRIC_STR      = "metric";
    private static final String SRC_TAG_STR     =  "srcTag";

    @Autowired
    private FastIndexService fastIndexService;

    @RequestMapping(path = "/getIndexInfo.do", method = RequestMethod.GET)
    public Result<JSONObject> getIndexInfo(@RequestParam(value = "template", required=true) String template,
                                           @RequestParam(value = "time", required=true) long time,
                                           @RequestParam(value = "hdfsSize", required=false, defaultValue="-1") long hdfsSize) {
        try {
            IndexFastIndexInfo info = fastIndexService.getIndexConfig(template, time, hdfsSize);
            return new Result<>(info.toJson());
        } catch (Throwable t) {
            return new Result<>(ResultType.FAIL, t.getMessage());
        }
    }


    @RequestMapping(path = "/startLoadData.do", method = RequestMethod.GET)
    public Result<String> startLoadData(@RequestParam(value = "template", required=true) String template,
                                        @RequestParam(value = "time", required=true) long time,
                                        @RequestParam(value = "hdfsDir", required=true) String hdfsDir,
                                        @RequestParam(value = "expanFactor", required=true) int expanFactor,
                                        @RequestParam(value = "hdfsUser", required=false, defaultValue="null") String hdfsUser,
                                        @RequestParam(value = "hdfsPasswd", required=false, defaultValue="null") String hdfsPasswd,
                                        @RequestParam(value = "srcTag", required=false) String srcTag) {
        try {
            fastIndexService.startLoadData(template, time, hdfsDir, expanFactor, hdfsUser, hdfsPasswd, srcTag);
            return new Result<>(ResultType.SUCCESS);
        } catch (Throwable t) {
            return new Result<>(ResultType.FAIL, t.getMessage());
        }
    }

    @RequestMapping(path = "/submitMapping.do", method = RequestMethod.POST)
    public Result<String> submitMapping(@RequestBody JSONObject param) throws Exception {
        try {
            String template = param.getString(TEMPLATE_STR);
            long time = Long.valueOf(param.getString(TIME_STR));
            long reduceId = Long.valueOf(param.getString(REDUCE_ID_STR));
            JSONObject mapping = param.getJSONObject(MAPPING_STR);
            String srcTag = param.getString(SRC_TAG_STR);

            fastIndexService.sumitMapping(srcTag, template, time, reduceId, mapping);

            return new Result<>(ResultType.SUCCESS);
        } catch (Throwable t) {
            if (t instanceof JSONException) {
                return new Result<>(ResultType.FAIL, "json解析失败, param:" + JSON.toJSONString(param));
            }
            return new Result<>(ResultType.FAIL, t.getMessage());
        }
    }



    @RequestMapping(path = "/submitMetric.do", method = RequestMethod.POST)
    public Result<String> submitMetric(@RequestBody JSONObject param) throws Exception {
        try {
            String template = param.getString(TEMPLATE_STR);
            long time = Long.valueOf(param.getString(TIME_STR));
            long reduceId = Long.valueOf(param.getString(REDUCE_ID_STR));
            String srcTag = param.getString(SRC_TAG_STR);
            JSONObject metric = param.getJSONObject(METRIC_STR);

            fastIndexService.submitMetric(srcTag, template, time, reduceId, metric);

            return new Result<>(ResultType.SUCCESS);
        } catch (Throwable t) {
            if (t instanceof JSONException) {
                return new Result<>(ResultType.FAIL, "json解析失败, param:" + JSON.toJSONString(param));
            }
            return new Result<>(ResultType.FAIL, t.getMessage());
        }
    }

    @RequestMapping(path = "/getMetricByIndex.do", method = RequestMethod.GET)
    public Result<JSONObject> getMetricByIndex(@RequestParam(value = "template", required=true) String template,
                                               @RequestParam(value = "time", required=true) long time,
                                               @RequestParam(value = "srcTag", required=false) String srcTag) throws Exception {
        try {
            JSONObject ret = fastIndexService.getAllMetrics(srcTag, template, time);
            return new Result<>(ret);
        } catch (Throwable t) {
            return new Result<>(ResultType.FAIL, t.getMessage());
        }
    }


    @RequestMapping(path = "/isFinished.do", method = RequestMethod.GET)
    public Result<Boolean> isFinished(@RequestParam(value = "template", required=true) String template,
                                      @RequestParam(value = "time", required=true) long time,
                                      @RequestParam(value = "srcTag", required=false) String srcTag) throws Exception {
        try {
            boolean isFinished = fastIndexService.isFinish(srcTag, template, time);
            return new Result<>(isFinished);

        } catch (Throwable t) {
            return new Result<>(ResultType.FAIL, t.getMessage());
        }
    }


    @RequestMapping(path = "/removeFinishTag.do", method = RequestMethod.GET)
    public Result<Boolean> removeFinishTag(@RequestParam(value = "template", required=true) String template,
                                           @RequestParam(value = "time", required=true) long time,
                                           @RequestParam(value = "srcTag", required=false) String srcTag) throws Exception {
        try {
            fastIndexService.removeFinishTag(srcTag, template, time);
            return new Result<>(ResultType.SUCCESS);

        } catch (Throwable t) {
            return new Result<>(ResultType.FAIL, t.getMessage());
        }
    }
}

