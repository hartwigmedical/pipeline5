package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import java.util.Objects;

public class GridssArgument {
    private final String key;
    private final String value;

    GridssArgument(final String key, final String value) {
        this.key = key.toUpperCase();
        this.value = value;
    }

    static GridssArgument tempDir() {
        return new GridssArgument("tmp_dir", System.getProperty("java.io.tmpdir"));
    }

    static GridssArgument blacklist(String blacklist) {
        return new GridssArgument("blacklist", blacklist);
    }

    static GridssArgument configFile(String configurationFile) {
        return new GridssArgument("configuration_file", configurationFile);
    }

    public String asBash() {
        return format("%s=%s", key, value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GridssArgument that = (GridssArgument) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}