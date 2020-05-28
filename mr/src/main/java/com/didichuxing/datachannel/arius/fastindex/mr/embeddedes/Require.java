package com.didichuxing.datachannel.arius.fastindex.mr.embeddedes;

class Require {
    static void require(boolean condition, String message) {
        if (!condition) {
            throw new InvalidSetupException(message);
        }
    }
}
