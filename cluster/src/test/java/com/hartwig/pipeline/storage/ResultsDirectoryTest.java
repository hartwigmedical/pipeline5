package com.hartwig.pipeline.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.ResultsDirectory;

import org.junit.Test;

public class ResultsDirectoryTest {

    @Test
    public void returnsOnlyNamespacedDirectory() {
        assertThat(ResultsDirectory.defaultDirectory().path()).isEqualTo("results");
    }

    @Test
    public void includesDirectoryAndNamespaceInResult() {
        assertThat(ResultsDirectory.defaultDirectory().path("subpath")).isEqualTo("results/subpath");
    }

    @Test
    public void avoidsDoubleSlashesWhenSubpathLeadsWithSlash() {
        assertThat(ResultsDirectory.defaultDirectory().path("/subpath")).isEqualTo("results/subpath");
    }
}