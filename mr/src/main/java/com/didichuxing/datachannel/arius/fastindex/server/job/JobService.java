package com.didichuxing.datachannel.arius.fastindex.server.job;

import com.didichuxing.datachannel.arius.fastindex.mr.utils.LogUtils;

public class JobService {
    private Thread thread;

    private FastIndexLoadDataParam fastIndexLoadDataParam = new FastIndexLoadDataParam();

    public void startSchedule() {
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FastIndexLoadDataCollector.handleJobTask(fastIndexLoadDataParam);
                } catch (Throwable t) {
                    LogUtils.error("FastIndexLoadDataCollector get exception", t);
                }

                try {
                    FastIndexOpIndexCollector.handleJobTask("");
                } catch (Throwable t) {
                    LogUtils.error("FastIndexOpIndexCollector get exception", t);
                }

                try {
                    Thread.sleep(60*1000);
                } catch (Throwable t) { }
            }
        });


        thread.setName("job-server");
        thread.setDaemon(true);
        thread.start();
    }
}
