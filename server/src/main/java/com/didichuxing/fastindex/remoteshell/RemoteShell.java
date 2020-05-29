package com.didichuxing.fastindex.remoteshell;

import java.util.List;

public interface RemoteShell {

        /*
         * 在对应主机上执执行对应shell脚本, 同时将args传入脚本
         * shell脚本在当前项目的resource目录下：shell/loadData.sh
         * @host 主机名
         * @args 参数
         *
         * @return 和本次任务对于的taskId
         */
        public long startShell(String host, List<String> args) throws Exception;

        /*
         * 脚本完成之后，获得脚本的输出，用于校验是否有异常
         * @param taskId 脚本单次执行的id，由starShell返回
         *
         * @return shell执行的结果
         */
        public String getShellOutput(long taskId) throws Exception;

        /*
         * 判断脚本是否执行完成
         * @param taskId 脚本单次执行的id，由startShell返回
         *
         * @param 脚本是否执行成功
         */
        public boolean isShellDone(long taskId);
}
