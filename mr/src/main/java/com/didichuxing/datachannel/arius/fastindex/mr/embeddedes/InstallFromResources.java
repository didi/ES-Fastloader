package com.didichuxing.datachannel.arius.fastindex.mr.embeddedes;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class InstallFromResources implements InstallationSource {

    private final URL resource;
    private final String version;

    InstallFromResources(String inResourcePath) {
        resource = Thread.currentThread().getContextClassLoader().getResource(inResourcePath);
        version = versionFromUrl(resource);
    }
    
    @Override
    public String determineVersion() {
        return version;
    }

    @Override
    public URL resolveDownloadUrl() {
        return resource;
    }

    private String versionFromUrl(URL url) {
        Pattern versionPattern = Pattern.compile("-([^/]*).zip");
        Matcher matcher = versionPattern.matcher(url.toString());
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Cannot find version in this archive name. Note that I was looking for zip archive with name in format: \"anyArchiveName-versionInAnyFormat.zip\". Examples of valid urls:\n" +
                "- elasticsearch-2.3.0.zip\n" +
                "- myDistributionOfElasticWithChangedName-1.0.0.zip");
    }
}
