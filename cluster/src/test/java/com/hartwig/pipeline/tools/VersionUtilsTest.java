package com.hartwig.pipeline.tools;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class VersionUtilsTest {

    @Test
    public void parsesMajorMinorVersion() {
        assertThat(VersionUtils.majorMinorVersion("5.3.1")).isEqualTo("5.3");
    }

    @Test
    public void noParsingOnMalformedVersion() {
        assertThat(VersionUtils.majorMinorVersion("blah")).isEqualTo("blah");
    }
}