package com.didichuxing.datachannel.arius.fastindex.embeddedes;

import java.util.function.Consumer;

/**
 * select java home to use when running elasticsearch
 */
public abstract class JavaHomeOption {

    public abstract boolean shouldBeSet();

    public abstract String getValue();

    public void ifNeedBeSet(Consumer<String> consumer) {
        if(shouldBeSet()) {
            consumer.accept(getValue());
        }
    }

    public static JavaHomeOption useSystem() {
        return new UseSystem();
    }

    public static JavaHomeOption inheritTestSuite() {
        return new Inherit();
    }

    public static JavaHomeOption path(String path) {
        return new Path(path);
    }

    /**
     * do not set an option, use the system wide set default
     */
    public static class UseSystem extends JavaHomeOption {

        public boolean shouldBeSet() {
            return false;
        }

        public String getValue() {
            return null;
        }
    }

    /**
     * inherit java home from the process running the tests
     */
    public static class Inherit extends JavaHomeOption {

        @Override
        public boolean shouldBeSet() {
            return true;
        }

        @Override
        public String getValue() {
            return System.getProperty("java.home");
        }
    }

    /**
     * explicitly set java home using a path, e.g.
     *
     * new JavaHomeOption.Path("/usr/lib/jvm/java-8-openjdk-amd64/")
     */
    public static class Path extends JavaHomeOption {

        String value;

        public Path(String value) {
            this.value = value;
        }

        @Override
        public boolean shouldBeSet() {
            return true;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

}
