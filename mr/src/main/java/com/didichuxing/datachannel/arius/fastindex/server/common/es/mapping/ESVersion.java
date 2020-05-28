package com.didichuxing.datachannel.arius.fastindex.server.common.es.mapping;

public enum ESVersion {
    ES233("es-version2.3.3"),
    ES501("es-version5.0.1"),
    ES651("es-version6.5.1");

    private String str;

    private ESVersion(String str) {
        this.str = str;
    }

    public String getStr() {
        return str;
    }

    public static ESVersion valueBy(String version) {

        if (version == null) {
            return null;
        }

        for (ESVersion esVersion : ESVersion.values()) {
            if (esVersion.getStr().contains(version)) {
                return esVersion;
            }
        }

        try {

            Integer versionBig = Integer.valueOf(version.replace(".", ""));

            if (versionBig < 5) {
                return ES233;
            }

            if (versionBig >= 6) {
                return ES651;
            }

            if (versionBig == 5) {
                return ES501;
            }

        } catch (Exception e) {

        }

        return null;
    }

    //    public static void main(String[] args) {
    //        System.out.println("2.3.3 : " + ESVersion.valueBy("2.3.3").getStr());
    //        System.out.println("6.5.1 : " + ESVersion.valueBy("6.5.1").getStr());
    //        System.out.println("6.6.1 : " + ESVersion.valueBy("6.6.1").getStr());
    //        System.out.println("3.3.0 : " + ESVersion.valueBy("7.5.0").getStr());
    //        System.out.println("5.5.0 : " + ESVersion.valueBy("5.5.0").getStr());
    //    }

}
