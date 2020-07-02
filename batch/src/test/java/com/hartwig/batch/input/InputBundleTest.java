package com.hartwig.batch.input;

import static java.util.Arrays.asList;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

public class InputBundleTest {
    private String key;
    private InputBundle victim;
    private InputFileDescriptor descriptor;

    @Before
    public void setup() {
        key = "my_key";
        InputFileDescriptor another = InputFileDescriptor.builder().name("some other key").inputValue("irrelevant")
                .billedProject("project").build();
        descriptor = InputFileDescriptor.builder().name(key).inputValue("irrelevant")
                .billedProject("project").build();
        victim = new InputBundle(asList(descriptor, another));
    }

    @Test
    public void shouldReturnInputWithKey() {
        assertThat(victim.get(key)).isEqualTo(descriptor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfInputWithKeyIsMissing() {
        victim.get("missing key");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfNullPassedToConstructor() {
        new InputBundle(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfEmptyListPassedToConstructor() {
        new InputBundle(new ArrayList<>());
    }
}