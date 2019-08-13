package com.didi.bigdata.mr2es.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 存放用户提交的参数信息
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CmdParam {

    private String key;

    /**
     * 是否必须
     */
    private boolean flag;

    /**
     * key描述
     */
    private String desc;

}

