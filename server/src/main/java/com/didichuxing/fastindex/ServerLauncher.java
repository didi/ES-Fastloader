package com.didichuxing.fastindex;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.ApplicationContext;

@ServletComponentScan
@SpringBootApplication(scanBasePackages = { "com.didichuxing.fastindex" })
public class ServerLauncher {

    public static void main(String[] args) {
        // 由于FilterRegistrationBean的注入，需要在加载完环境配置文件信息之后，因此先根据传入参数，先加载
        ApplicationContext ctx = SpringApplication.run(ServerLauncher.class, args);
    }
}
