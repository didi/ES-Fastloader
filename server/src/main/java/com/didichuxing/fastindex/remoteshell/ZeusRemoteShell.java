package com.didichuxing.fastindex.remoteshell;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.didichuxing.fastindex.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class ZeusRemoteShell implements RemoteShell {
        private static final String TASK_STR = "{ \"tpl_id\":%d, \"account\":\"%s\", \"hosts\": [ \"%s\" ], " +
                "\"batch\":1, \"tolerance\":0, \"pause\":\"\", \"timeout\":%d, \"args\":\"%s\", \"action\":\"start\" }";
        private static final String user = "arius";
        private static final long zeusTemplaeId = 1246;
        /*
         * 在对应主机上执执行对应shell脚本
         * shell脚本在当前项目的resource目录下：shell/loadData.sh
         * @tplID 脚本id
         * @user 执行脚本的用户名
         * @server 执行脚本的主键名
         * @timeout 执行超时时间
         * @args 脚本的参数，具体见脚本文件
         */
        private static final String START_TASK_URL_STR = "http://zeus.intra.xiaojukeji.com/api/task?token=xxx";
        @Override
        public long startShell(String host, List<String> args) throws Exception {
                host = host+".docker.ys";

                String argsStr = StringUtils.join(args, ",,");

                String jsonParam = String.format(TASK_STR, zeusTemplaeId, user, host, 1200, argsStr);
                String resp = HttpUtil.doHttp(START_TASK_URL_STR, null, jsonParam,  HttpUtil.HttpType.POST);

                return Long.valueOf(getData(resp));
        }

        /*
         * 脚本完成之后，获得脚本的输出，用于校验是否有异常
         * @param taskId 脚本单次执行的id，由startTask返回
         */
        private static final String GET_STDOUT_URL_FORMAT = "http://zeus.intra.xiaojukeji.com/api/task/%d/stdouts.json";
        @Override
        public String getShellOutput(long taskId) throws Exception {
                String url = String.format(GET_STDOUT_URL_FORMAT, taskId);
                String resp = HttpUtil.doHttp(url, null, null, HttpUtil.HttpType.GET);

                return getData(resp);
        }

        /*
         * 判断脚本是否执行完成
         * @param taskId 脚本单次执行的id，由startTask返回
         */
        private static final String GET_STATE_URL_FORMAT = "http://zeus.intra.xiaojukeji.com/api/task/%d/state";
        @Override
        public boolean isShellDone(long taskId) {
                try {
                        String url = String.format(GET_STATE_URL_FORMAT, taskId);
                        String resp = HttpUtil.doHttp(url, null, null, HttpUtil.HttpType.GET);

                        // eg {"data":"done","msg":""}
                        JSONObject obj = JSONObject.parseObject(resp);
                        String state = obj.getString("data");
                        if ("done".equalsIgnoreCase(state)) {
                                return true;
                        } else {
                                return false;
                        }
                } catch (Throwable t) {
                        log.warn("zeus check is done getException, taskId:" + taskId, t);
                        return false;
                }
        }

        private static final String MSG_STR = "msg";
        private static final String DATA_STR = "data";

        private String getData(String resp) throws Exception {
                JSONObject obj = JSON.parseObject(resp);
                String msg = obj.getString(MSG_STR);
                if (!StringUtils.isBlank(msg)) {
                        throw new Exception("zeus error, resp:" + resp + ", msg:" + msg);
                }

                return obj.getString(DATA_STR);
        }
}
