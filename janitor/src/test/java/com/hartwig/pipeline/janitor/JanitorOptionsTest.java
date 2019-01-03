package com.hartwig.pipeline.janitor;

import static com.hartwig.pipeline.janitor.JanitorOptions.DEFAULT_INTERVAL_SECONDS;
import static com.hartwig.pipeline.janitor.JanitorOptions.DEFAULT_PRIVATE_KEY_PATH;
import static com.hartwig.pipeline.janitor.JanitorOptions.DEFAULT_PROJECT;
import static com.hartwig.pipeline.janitor.JanitorOptions.DEFAULT_REGION;
import static com.hartwig.pipeline.janitor.JanitorOptions.INTERVAL_FLAG;
import static com.hartwig.pipeline.janitor.JanitorOptions.PRIVATE_KEY_FLAG;
import static com.hartwig.pipeline.janitor.JanitorOptions.PROJECT_FLAG;
import static com.hartwig.pipeline.janitor.JanitorOptions.REGION_FLAG;
import static com.hartwig.pipeline.janitor.JanitorOptions.from;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.Test;

public class JanitorOptionsTest {

    private static final String PRIVATE_KEY = "privatekey";
    private static final String INTERVAL = "5";
    private static final String PROJECT = "project";
    private static final String REGION = "region";

    @Test
    public void defaultsSetWhenArgumentsEmpty() throws Exception {
        Optional<Arguments> maybeArguments = from(new String[] {});
        assertThat(maybeArguments).isPresent()
                .hasValue(Arguments.builder()
                        .privateKeyPath(DEFAULT_PRIVATE_KEY_PATH)
                        .project(DEFAULT_PROJECT)
                        .region(DEFAULT_REGION)
                        .intervalInSeconds(Integer.parseInt(DEFAULT_INTERVAL_SECONDS))
                        .build());
    }

    @Test
    public void setsArgumentsToCorrectValues() throws Exception {
        Optional<Arguments> maybeArguments =
                from(new String[] { flag(PRIVATE_KEY_FLAG), PRIVATE_KEY, flag(INTERVAL_FLAG), INTERVAL, flag(PROJECT_FLAG), PROJECT,
                        flag(REGION_FLAG), REGION });
        assertThat(maybeArguments).isPresent()
                .hasValue(Arguments.builder()
                        .privateKeyPath(PRIVATE_KEY)
                        .project(PROJECT)
                        .region(REGION)
                        .intervalInSeconds(Integer.parseInt(INTERVAL))
                        .build());
    }

    @Test
    public void returnsEmptyOptionalOnBadArguments() {
        Optional<Arguments> maybeArguments = from(new String[] { flag("bad"), "argument" });
        assertThat(maybeArguments).isEmpty();
    }

    private String flag(final String flag) {
        return "-" + flag;
    }
}