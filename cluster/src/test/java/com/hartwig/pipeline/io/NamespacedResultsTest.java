package com.hartwig.pipeline.io;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class NamespacedResultsTest {

    @Test
    public void returnsOnlyNamespacedDirectory() {
        assertThat(NamespacedResults.of("test").path()).isEqualTo("results/test");
    }

    @Test
    public void includesDirectoryAndNamespaceInResult() {
        assertThat(NamespacedResults.of("test").path("subpath")).isEqualTo("results/test/subpath");
    }

    @Test
    public void avoidsDoubleSlashesWhenSubpathLeadsWithSlash() {
        assertThat(NamespacedResults.of("test").path("/subpath")).isEqualTo("results/test/subpath");
    }
}