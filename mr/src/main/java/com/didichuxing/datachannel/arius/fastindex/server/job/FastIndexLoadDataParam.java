package com.didichuxing.datachannel.arius.fastindex.server.job;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class FastIndexLoadDataParam {
    private int maxCocurrent = 20 ;

    private int nodeMaxCocurrent = 10;

    private int waitTimeout = 20*60*1000;
}
