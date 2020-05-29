package com.didichuxing.fastindex.server;

import com.didichuxing.fastindex.job.FastIndexLoadDataCollector;
import com.didichuxing.fastindex.job.FastIndexOpIndexCollector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Slf4j
@Component
public class SchduleService {
    @Autowired
    private FastIndexLoadDataCollector fastIndexLoadDataCollector;

    @Autowired
    private FastIndexOpIndexCollector fastIndexOpIndexCollector;

    private Thread thread;

    @PostConstruct
    public void init() {
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        fastIndexLoadDataCollector.handleJobTask();
                    } catch (Throwable t) {
                        // TODO add log
                    }

                    try {
                        fastIndexOpIndexCollector.handleJobTask();
                    } catch (Throwable t) {
                        // TODO add log
                    }

                    try {
                        Thread.sleep(60*1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        thread.setDaemon(false);
        thread.setName("schduler");
        thread.start();
    }
}
