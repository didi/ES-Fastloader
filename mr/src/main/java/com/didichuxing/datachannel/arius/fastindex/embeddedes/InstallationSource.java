package com.didichuxing.datachannel.arius.fastindex.embeddedes;

import java.net.URL;

interface InstallationSource {
    String determineVersion();

    URL resolveDownloadUrl();
}

