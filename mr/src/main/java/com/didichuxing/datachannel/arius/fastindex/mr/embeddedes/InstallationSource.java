package com.didichuxing.datachannel.arius.fastindex.mr.embeddedes;

import java.net.URL;

interface InstallationSource {
    String determineVersion();

    URL resolveDownloadUrl();
}

