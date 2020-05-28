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
                                        @RequestParam(value = "hdfsUser", required=true) String hdfsUser,
                                        @RequestParam(value = "hdfsPasswd", required=true) String hdfsPasswd) {
        try {
            fastIndexService.startLoadData(template, time, hdfsDir, expanFactor, hdfsUser, hdfsPasswd);
            return new Result<>(ResultType.SUCCESS);
        } catch (Throwable t) {
            return new Result<>(ResultType.FAIL, t.getMessage());
        }
    }

    @RequestMapping(path = "/isFinished.do", method = RequestMethod.GET)
    public Result<Boolean> isFinished(@RequestParam(value = "template", required=true) String template,
                                      @RequestParam(value = "time", required=true) long time) throws Exception {
        try {
            boolean isFinished = fastIndexService.isFinish(template, time);
            return new Result<>(isFinished);

        } catch (Throwable t) {
            return new Result<>(ResultType.FAIL, t.getMessage());
        }
    }


    @RequestMapping(path = "/removeFinishTag.do", method = RequestMethod.GET)
    public Result<Boolean> removeFinishTag(@RequestParam(value = "template", required=true) String template,
                                           @RequestParam(value = "time", required=true) long time) throws Exception {
        try {
            fastIndexService.removeFinishTag(template, time);
            return new Result<>(ResultType.SUCCESS);

        } catch (Throwable t) {
            return new Result<>(ResultType.FAIL, t.getMessage());
        }
    }
}

