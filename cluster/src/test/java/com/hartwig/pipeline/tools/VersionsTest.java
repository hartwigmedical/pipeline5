package com.hartwig.pipeline.tools;


import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class VersionsTest {

    @Test
    public void parsesMajorMinorVersion() {
        assertThat(Versions.majorMinorVersion("5.3.1")).isEqualTo("5.3");
    }

    @Test
    public void noParsingOnMalformedVersion() {
        assertThat(Versions.majorMinorVersion("blah")).isEqualTo("blah");
    }
}