package com.didi.bigdata.mr2es.utils;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * 解析命令行
 */
@Slf4j
public class Cmd {
    private Options options;
    private String name;
    private CommandLine line;
    private List<CmdParam> keys = Lists.newArrayList();

    public Cmd(String cmdName) {
        this.name = "hadoop jar " + cmdName;
        this.options = new Options();
    }

    /**
     * 参数key以"--"开头，例如--reduce_num，命令必须传入
     *
     * @param key
     * @param desc
     */
    public void addParam(String key, String desc) {
        options.addOption(OptionBuilder.withLongOpt(key)
                .withDescription(desc)
                .hasArg()
                .withArgName("key=value")
                .create());
        CmdParam param = new CmdParam(key, true, desc);
        keys.add(param);
    }

    /**
     * 解析传入的参数
     *
     * @param args
     */
    public void parse(String[] args) {
        CommandLineParser parser = new PosixParser();
        try {
            this.line = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("参数解析错误，请参考以下格式:");
            printHelp();
            System.exit(-1);
        }
        List<String> unExistKeys = Lists.newArrayList();
        for (CmdParam key : keys) {
            if (key.isFlag() && (!hasArg(key.getKey()) ||
                    StringUtils.isBlank(this.getArgValue(key.getKey()).trim()))) {
                unExistKeys.add("--" + key.getKey() + "\t" + key.getDesc());
            }
        }
        if (unExistKeys.size() > 0) {
            log.info("以下参数必须在命令行中指定，而您未指定：");
            for (String key : unExistKeys) {
                log.info(key);
            }
            System.exit(-1);
        }
        log.info("您使用的命令参数为：");
        for (CmdParam key : keys) {
            if (hasArg(key.getKey())) {
                log.info("--" + key.getKey() + " = "
                        + this.getArgValue(key.getKey()));
            }
        }
    }

    /**
     * 打印参数帮助
     */
    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(this.name, this.options);
    }

    /**
     * 判断是否存在参数
     *
     * @param key
     * @return
     */
    public boolean hasArg(String key) {
        return line.hasOption(key);
    }

    /**
     * 得到key对应的值
     *
     * @param key
     * @return
     */
    public String getArgValue(String key) {
        return line.getOptionValue(key);
    }

    /**
     * 执行cmd命令
     * @param cmd
     * @throws Exception
     */
    public static void execCmd(String cmd) throws Exception {
        String[] cmds = {"/bin/sh", "-c", cmd};
        Process pro = Runtime.getRuntime().exec(cmds);
        pro.waitFor();
        InputStream in = pro.getInputStream();
        BufferedReader read = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = read.readLine()) != null) {
            log.info("cmd:{},result:{}", cmd, line);
        }
    }
}

