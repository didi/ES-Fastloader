package com.didichuxing.datachannel.arius.fastindex.embeddedes;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.io.FileUtils.forceMkdir;
import static org.apache.commons.io.FileUtils.getFile;

class ElasticDownloader {
    private static final Logger logger = LoggerFactory.getLogger(ElasticDownloader.class);
    private static final String ELS_PACKAGE_STATUS_FILE_SUFFIX = "-downloaded";

    private final InstallationDescription installationDescription;

    ElasticDownloader(InstallationDescription installationDescription) {
        this.installationDescription = installationDescription;
    }

    Path download() throws IOException, InterruptedException {
        prepareDirectories();
        return downloadElastic();
    }

    private void prepareDirectories() throws IOException {
        forceMkdir(getDownloadDirectory());
    }

    private File getDownloadDirectory() {
        return getFile(installationDescription.getDownloadDirectory());
    }

    private Path downloadElastic() throws IOException {
        URL source = installationDescription.getDownloadUrl();
        File target = new File(getDownloadDirectory(), constructLocalFileName(source));
        File statusFile = new File(target.getParentFile(), target.getName() + ELS_PACKAGE_STATUS_FILE_SUFFIX);
        removeBrokenDownload(target, statusFile);
        if (!target.exists()) {
            proceedWithDownload(source, target, statusFile, installationDescription.getDownloaderConnectionTimeoutInMs(), installationDescription.getDownloaderReadTimeoutInMs());
        } else if (!statusFile.exists() && maybeDownloading(target)) {
            waitForDownload(target, statusFile);
        } else if (!statusFile.exists()) {
            throw new IOException("Broken download. File '" + target + "' exits but status '" + statusFile + "' file wash not created");
        } else {
            logger.info("Download skipped");
        }
        return target.toPath();
    }

    private String constructLocalFileName(URL url) {
        String path = url.getPath();
        if (path.isEmpty()) {
            return RandomStringUtils.randomAlphanumeric(10);
        }
        return FilenameUtils.getName(path);
    }

    private void removeBrokenDownload(File target, File statusFile) throws IOException {
        if (target.exists() && !statusFile.exists() && !maybeDownloading(target)) {
            logger.info("Removing broken download file {}", target);
            FileUtils.forceDelete(target);
        }
    }

    private boolean maybeDownloading(File target) {
        // Check based on assumption that if other thread or jvm is currently downloading file on disk should be modified
        // at least every 10 seconds as new data is being downloaded. This will not work on file system
        // without support for lastmodified field or on very slow internet connection
        return System.currentTimeMillis() - target.lastModified() < TimeUnit.SECONDS.toMillis(10L);
    }

    private void proceedWithDownload(URL source, File target, File statusFile, int connectionTimeout, int readTimeout) throws IOException {
        logger.info("Downloading {} to {} ...", source, target);
        copyURLToFile(source, target, connectionTimeout, readTimeout, installationDescription.getDownloadProxy());
        FileUtils.touch(statusFile);
        logger.info("Download complete");
    }

    private void copyURLToFile(URL source,
                               File destination,
                               int connectionTimeout,
                               int readTimeout,
                               Proxy proxy) throws IOException {
        final URLConnection connection = proxy != null ? source.openConnection(proxy) : source.openConnection();
        connection.setConnectTimeout(connectionTimeout);
        connection.setReadTimeout(readTimeout);
        FileUtils.copyInputStreamToFile(connection.getInputStream(), destination);
    }

    private void waitForDownload(File target, File statusFile) throws IOException {
        boolean downloaded;
        do {
            logger.info("File {} (size={}) is probably being downloaded by another thread/jvm. Waiting ...", target, target.length());
            downloaded = FileUtils.waitFor(statusFile, 30);
        } while (!downloaded && maybeDownloading(target));
        if (!downloaded) {
            throw new IOException("Broken download. Another party probably failed to download " + target);
        }
        logger.info("File was downloaded by another thread/jvm. Download skipped");
    }

}
