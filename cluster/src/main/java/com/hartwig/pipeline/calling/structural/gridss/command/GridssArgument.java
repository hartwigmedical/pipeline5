package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import java.util.Objects;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class GridssArgument implements BashCommand {
    private final String key;
    private final String value;

    GridssArgument(final String key, final String value) {
        this.key = key.toUpperCase();
        this.value = value;
    }

    @Override
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
