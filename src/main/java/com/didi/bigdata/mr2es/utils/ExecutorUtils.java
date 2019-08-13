package com.didi.bigdata.mr2es.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author WangZhuang
 * @date 2018年10月1日
 *
 */
@Slf4j
public class ExecutorUtils {

	private static ExecutorService executor = Executors
            .newFixedThreadPool(5);

	public static ExecutorService getExecutorService() {
		return executor;
	}
	
	public static void stop() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(100, TimeUnit.SECONDS)) {
                log.error("executor util not stop, wait time 100s!");
            } else {
                log.info("executor util stop success!");
            }
        } catch (InterruptedException e) {
            log.error("executor util stop error!", e);
        }
    }

}
