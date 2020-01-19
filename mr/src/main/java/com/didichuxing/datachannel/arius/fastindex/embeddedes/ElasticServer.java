package com.didichuxing.datachannel.arius.fastindex.embeddedes;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.commons.lang3.Validate.isTrue;

class ElasticServer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticServer.class);

    private final String esJavaOpts;
    private final File installationDirectory;
    private final File executableFile;
    private final long startTimeoutInMs;
    private final boolean cleanInstallationDirectoryOnStop;

    private boolean started;
    private final Object startedLock = new Object();

    private Process elastic;
    private Thread ownerThread;
    private volatile int pid = -1;
    private volatile int httpPort = -1;
    private volatile int transportTcpPort = -1;
    private JavaHomeOption javaHome;

    ElasticServer(String esJavaOpts, File installationDirectory, File executableFile, long startTimeoutInMs, boolean cleanInstallationDirectoryOnStop, JavaHomeOption javaHome) {
        this.esJavaOpts = esJavaOpts;
        this.installationDirectory = installationDirectory;
        this.executableFile = executableFile;
        this.startTimeoutInMs = startTimeoutInMs;
        this.cleanInstallationDirectoryOnStop = cleanInstallationDirectoryOnStop;
        this.javaHome = javaHome;
    }

    void start() throws InterruptedException {
        startElasticProcess();
        installExitHook();
        waitForElasticToStart();
    }

    void stop() {
        try {
            stopElasticServer();
            finalizeClose();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    boolean isStarted() {
        return started;
    }

    private void deleteInstallationDirectory() {
        try {
            deleteDirectory(installationDirectory);
        } catch (IOException e) {
            throw new EmbeddedElasticsearchStartupException("Could not delete data directory of embedded elasticsearch server. Possibly an instance is running.", e);
        }
    }

    private void startElasticProcess() {
        ownerThread = new Thread(() -> {
            try {
                synchronized (this) {
                    ProcessBuilder builder = new ProcessBuilder();
                    builder.environment().put("ES_JAVA_OPTS", esJavaOpts);
                    javaHome.ifNeedBeSet(javaHomeValue -> builder.environment().put("JAVA_HOME", javaHomeValue));
                    builder.redirectErrorStream(true);
                    builder.command(elasticExecutable());
                    elastic = builder.start();
                }
                BufferedReader outputStream = new BufferedReader(new InputStreamReader(elastic.getInputStream(), UTF_8));
                String line;
                while ((line = readLine(outputStream)) != null) {
                    logger.info(line);
                    parseElasticLogLine(line);
                }
            } catch (Exception e) {
                throw new EmbeddedElasticsearchStartupException(e);
            }
        }, "EmbeddedElsHandler");
        ownerThread.start();
    }

    private String readLine(BufferedReader outputStream) {
        try {
            return outputStream.readLine();
        } catch (IOException e) {
            // TODO: catching all types of IOException is not clean, any better solution?
            return null;
        }
    }

    private void installExitHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "ElsInstanceCleaner"));
    }

    private String elasticExecutable() {
        return executableFile.getAbsolutePath();
    }

    private void waitForElasticToStart() throws InterruptedException {
        logger.info("Waiting for ElasticSearch to start...");
        long waitUntil = System.currentTimeMillis() + startTimeoutInMs;

        synchronized (startedLock) {
            boolean timedOut = false;
            while (!started && !timedOut && (elastic == null || elastic.isAlive())) {
                startedLock.wait(100);
                timedOut = System.currentTimeMillis() > waitUntil;
            }
            if (!started) {
                String message = timedOut ? "Failed to start elasticsearch within time-out" : "Failed to start elasticsearch. Check previous logs for details";
                throw new EmbeddedElasticsearchStartupException(message);
            }
        }

        logger.info("ElasticSearch started...");
    }

    private void parseElasticLogLine(String line) {
        if (started) {
            return;
        }
        if (line.contains("] started")) {
            signalElasticStarted();
        } else if (line.contains(", pid[")) {
            tryExtractPid(line);
        } else if (line.contains("publish_address") && (line.contains("[http") || line.contains("HttpServer"))) {
            tryExtractHttpPort(line);
        } else if (line.contains("publish_address") && (line.contains("[transport") || line.contains("TransportService"))) {
            tryExtractTransportTcpPort(line);
        }
    }

    private void signalElasticStarted() {
        synchronized (startedLock) {
            started = true;
            startedLock.notifyAll();
        }
    }

    private void tryExtractPid(String line) {
        Matcher matcher = Pattern.compile("pid\\[(\\d+)]").matcher(line);
        isTrue(matcher.find());
        pid = Integer.parseInt(matcher.group(1));
        logger.info("Detected Elasticsearch PID : " + pid);
    }

    private void tryExtractHttpPort(String line) {
        Matcher matcher = Pattern.compile("publish_address \\{.*?:(\\d+).?}").matcher(line);
        isTrue(matcher.find());
        httpPort = Integer.parseInt(matcher.group(1));
        logger.info("Detected Elasticsearch http port : " + httpPort);
    }

    private void tryExtractTransportTcpPort(String line) {
        Matcher matcher = Pattern.compile("publish_address \\{.*?:(\\d+).?}").matcher(line);
        isTrue(matcher.find());
        transportTcpPort = Integer.parseInt(matcher.group(1));
        logger.info("Detected Elasticsearch transport tcp port : " + transportTcpPort);
    }

    private void stopElasticServer() throws IOException, InterruptedException {
        logger.info("Stopping elasticsearch server...");
        if (pid > -1) {
            stopElasticGracefully();
        }
        pid = -1;
        if (elastic != null) {
            int rc = elastic.waitFor();
            logger.info("Elasticsearch exited with RC " + rc);
        }
        elastic = null;
        if (ownerThread != null) {
            ownerThread.join();
        }
        ownerThread = null;
    }

    private void stopElasticGracefully() throws IOException {
        if (SystemUtils.IS_OS_WINDOWS) {
            stopElasticOnWindows();
        } else {
            elastic.destroy();
        }
    }

    private void stopElasticOnWindows() throws IOException {
        Runtime.getRuntime().exec("taskkill /f /pid " + pid);
    }

    private void finalizeClose() {
        if (this.cleanInstallationDirectoryOnStop) {
            logger.info("Removing installation directory...");
            deleteInstallationDirectory();
        }
        logger.info("Finishing...");
        started = false;
    }

    int getHttpPort() {
        return httpPort;
    }

    int getTransportTcpPort() {
        return transportTcpPort;
    }
}
