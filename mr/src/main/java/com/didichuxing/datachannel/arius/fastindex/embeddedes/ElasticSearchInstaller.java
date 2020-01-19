package com.didichuxing.datachannel.arius.fastindex.embeddedes;

import org.apache.commons.io.FileUtils;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.FileUtils.forceMkdir;
import static org.apache.commons.io.FileUtils.getFile;
import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;

class ElasticSearchInstaller {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchInstaller.class);
    private static final String ELS_PACKAGE_PREFIX = "elasticsearch-";

    private final InstanceSettings instanceSettings;
    private final InstallationDescription installationDescription;
    private final ElasticDownloader elasticDownloader;

    ElasticSearchInstaller(InstanceSettings instanceSettings, InstallationDescription installationDescription) {
        this.instanceSettings = instanceSettings;
        this.installationDescription = installationDescription;
        this.elasticDownloader = new ElasticDownloader(installationDescription);
    }

    File getExecutableFile() {
        return fileRelativeToInstallationDir("bin", systemDependentExtension("elasticsearch"));
    }

    File getInstallationDirectory() {
        return getFile(installationDescription.getInstallationDirectory(), ELS_PACKAGE_PREFIX + installationDescription.getVersion());
    }

    void install() throws IOException, InterruptedException {
        Path downloadedTo = elasticDownloader.download();
        prepareDirectories();
        installElastic(downloadedTo);
        configureElastic();
        installPlugins();
    }

    private void prepareDirectories() throws IOException {
        forceMkdir(getInstallationDirectory());
    }

    private void installElastic(Path downloadedTo) throws IOException {
        File destination = getInstallationDirectory().getParentFile();
        FileUtils.forceDelete(getInstallationDirectory());
        logger.info("Installing Elasticsearch" + " into " + destination + "...");
        try {
            unzip(downloadedTo, destination);
            logger.info("Done");
        } catch (IOException e) {
            logger.info("Failure : " + e);
            throw new EmbeddedElasticsearchStartupException(e);
        }
    }

    private void unzip(Path downloadedTo, File destination) throws IOException {
        Archiver archiver = ArchiverFactory.createArchiver("zip");
        archiver.extract(downloadedTo.toFile(), destination);
    }

    private void configureElastic() throws IOException {
        File elasticsearchYml = getFile(getInstallationDirectory(), "config", "elasticsearch.yml");
        FileUtils.writeStringToFile(elasticsearchYml, instanceSettings.toYaml(), UTF_8);
    }

    private void installPlugins() throws IOException, InterruptedException {
        File pluginManager = pluginManagerExecutable();
        Set<String> alreadyInstalledPlugins = getAlreadyInstalledPlugins();
        for (InstallationDescription.Plugin plugin : installationDescription.getPlugins()) {
            if (isPluginInstalled(plugin, (alreadyInstalledPlugins))) {
                logger.info("> Plugin " + plugin.getPluginName() + " already installed, skipping");
            } else {
                installPlugin(pluginManager, plugin);
            }
        }
    }

    private Set<String> getAlreadyInstalledPlugins() {
        File pluginsDir = new File(getInstallationDirectory(), "plugins");
        String[] pluginList = pluginsDir.list();
        if (pluginList != null) {
            return Stream.of(pluginList).collect(Collectors.toSet());
        } else {
            return Collections.emptySet();
        }
    }

    private void installPlugin(File pluginManager, InstallationDescription.Plugin plugin) throws IOException, InterruptedException {
        logger.info("> " + pluginManager + " install " + plugin.getExpression());
        ProcessBuilder builder = new ProcessBuilder();
        builder.redirectOutput(Redirect.PIPE);
        builder.redirectErrorStream(true);
        builder.command(prepareInstallCommand(pluginManager, plugin));
        Process process = builder.start();
        BufferedReader bReader = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8));
        String line;
        while ((line = bReader.readLine()) != null) {
            logger.info(String.format("Plugin install %s: %s", plugin, line));
        }
        if (process.waitFor() != 0) {
            throw new EmbeddedElasticsearchStartupException("Unable to install plugin: " + plugin);
        }
    }

    private static boolean isPluginInstalled(InstallationDescription.Plugin plugin, Set<String> alreadyInstalledPlugins) {
        return alreadyInstalledPlugins.contains(plugin.getPluginName());
    }

    private String[] prepareInstallCommand(File pluginManager, InstallationDescription.Plugin plugin) {
        if (installationDescription.versionIs1x() && plugin.expressionIsUrl()) {
            return new String[]{pluginManager.getAbsolutePath(), "--install", plugin.getPluginName(), "--url", plugin.getExpression()};
        }
        if (installationDescription.versionIs1x() || installationDescription.versionIs2x()) {
            return new String[]{pluginManager.getAbsolutePath(), "install", plugin.getExpression()};
        }
        return new String[]{pluginManager.getAbsolutePath(), "install", "--batch", plugin.getExpression()};
    }

    private File pluginManagerExecutable() throws IOException {
        File elasticsearchPlugin = fileRelativeToInstallationDir("bin", systemDependentExtension("elasticsearch-plugin"));
        File pluginManager;
        if (elasticsearchPlugin.exists()) {
            pluginManager = elasticsearchPlugin;
        } else {
            pluginManager = fileRelativeToInstallationDir("bin", systemDependentExtension("plugin"));
        }
        return pluginManager;
    }

    private String systemDependentExtension(String baseFileName) {
        return baseFileName + (IS_OS_WINDOWS ? ".bat" : "");
    }

    private File fileRelativeToInstallationDir(String... path) {
        return getFile(getInstallationDirectory(), path);
    }

}
