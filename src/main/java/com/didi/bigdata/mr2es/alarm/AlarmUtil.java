package com.didi.bigdata.mr2es.alarm;

import java.util.HashMap;
import java.util.Map;

import com.didi.bigdata.mr2es.utils.Constants;
import com.didi.bigdata.mr2es.utils.HttpUtil;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import com.alibaba.fastjson.JSONObject;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 监控告警
 */
@Slf4j
public class AlarmUtil {

    private String alarmGroup;
    private String level;
    private int ifCall;
    @Setter
    private static String profile = "";

    @Getter
    private static AlarmUtil alarmUtil = new AlarmUtil("alarm", AlarmLevel.LEVEL1,
            AlarmIfCall.CALL_NO);

    public AlarmUtil(String alarmGroup, AlarmLevel alarmLevel, AlarmIfCall alarmIfCall) {
        this.alarmGroup = alarmGroup;
        level = alarmLevel.getRemark() + profile;
        ifCall = alarmIfCall.getLevel();
    }

    public void exec(String alarmId, String content) {
        String currentTime = DateTime.now().toString(Constants.DATE_FORMAT);
        String odinUrl = Constants.ODIN_URL;
        Map<String, Object> requestParams = new HashMap<>(6);
        requestParams.put("state", ifCall);
        requestParams.put("subject", content);
        requestParams.put("groups", new String[]{alarmGroup});
        requestParams.put("level", level);
        // 报警策略，和 AlarmLevel、AlarmIfCall 挂钩
        requestParams.put("app", "bis_alarm");
        Map<String, Object> contentMap = new HashMap<>(5);
        contentMap.put("name", content);
        contentMap.put("time", currentTime);
        contentMap.put("measurement", alarmId);
        contentMap.put("content", content);
        contentMap.put("level", level);
        requestParams.put("content", contentMap);
        try {
            String res = HttpUtil.postJsonEntity(odinUrl,
                    new JSONObject(requestParams));
            log.info("alarm send res:{}", res);
            if (StringUtils.isBlank(res)) {
                log.error("alarm send error!");
                return;
            }
            JSONObject jsonObject = JSONObject.parseObject(res);
            if (jsonObject.getString("status").equals("fail")) {
                log.info("alarm send error!");
                return;
            }
        } catch (Exception e) {
            log.error("alarmGroup:{}, alarmId:{} failed to send warning: {}",
                    alarmGroup, alarmId, e);
        }
        log.info("alarmGroup:{}, alarmId:{} success to send warning!",
                alarmGroup, alarmId);
    }

    public static void main(String[] args) {
        AlarmUtil util = new AlarmUtil("es_fastload",
                AlarmLevel.LEVEL1, AlarmIfCall.CALL_NO);
    }
}
