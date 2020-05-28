package com.didichuxing.datachannel.arius.fastindex.mr.embeddedes;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class InstallFromDirectUrl implements InstallationSource {
    
    private final URL downloadUrl;
    private final String version;

    InstallFromDirectUrl(URL downloadUrl) {
        this.downloadUrl = downloadUrl;
        this.version = versionFromUrl(downloadUrl);
    }

    @Override
    public URL resolveDownloadUrl() {
        return downloadUrl;
    }

    @Override
    public String determineVersion() {
        return version;
    }

    private String versionFromUrl(URL url) {
        Pattern versionPattern = Pattern.compile("-([^/]*).zip");
        Matcher matcher = versionPattern.matcher(url.toString());
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Cannot find version in this url. Note that I was looking for zip archive with name in format: \"anyArchiveName-versionInAnyFormat.zip\". Examples of valid urls:\n" +
                "- http://example.com/elasticsearch-2.3.0.zip\n" +
                "- http://example.com/myDistributionOfElasticWithChangedName-1.0.0.zip");
    }
}
