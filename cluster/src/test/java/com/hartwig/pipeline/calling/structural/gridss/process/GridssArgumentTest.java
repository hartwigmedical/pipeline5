package com.hartwig.pipeline.calling.structural.gridss.process;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GridssArgumentTest {
    private String key;
    private String value;
    private GridssArgument argument;

    @Before
    public void setup() {
        key = "key";
        value = "value";
        argument = new GridssArgument(key, value);
    }

    @Test
    public void shouldReturnKeyCapitalisedWithEqualityBetweenKeyAndValue() {
        assertThat(argument.asBash()).isEqualTo("KEY=value");
    }

    @Test
    public void shouldCapitaliseKeyInConstructor() {
        assertThat(argument).isEqualTo(new GridssArgument(key.toUpperCase(), value));
    }

    @Test
    public void equalInstancesShouldReturnEqualHashCodes() {
        assertThat(argument.hashCode()).isEqualTo(new GridssArgument(key, value).hashCode());
    }

    @Test
    public void equalInstancesShouldCompareEqual() {
        assertThat(argument).isEqualTo(new GridssArgument(key, value));

    }

    @Test
    public void nonEqualInstancesShouldReturnNonEqualHashCodes() {
        assertThat(argument.hashCode()).isNotEqualTo(new GridssArgument(key + "a", value + "b").hashCode());
    }

    @Test
    public void nonEqualInstancesShouldNotCompareEqual() {
        assertThat(argument).isNotEqualTo(new GridssArgument(key + "b", value + "c"));
    }
}