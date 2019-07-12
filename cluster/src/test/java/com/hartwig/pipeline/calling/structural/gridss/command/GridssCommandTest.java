package com.hartwig.pipeline.calling.structural.gridss.command;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class GridssCommandTest {
    @Test
    public void shouldExtractBasenameForFileDroppingFinalExtension() {
        String completeFilename = "/some/file/somewhere/with.extension";
        assertThat(GridssCommand.basenameNoExtensions(completeFilename)).isEqualTo("with");
    }

    @Test
    public void shouldNotChokeWhenAskedToDropExtensionsFromBasenameIfThereAreNone() {
        String completeFileName = "/some/filename";
        assertThat(GridssCommand.basenameNoExtensions(completeFileName)).isEqualTo("filename");
    }

    @Test
    public void shouldDropMultipleExtensionsFromBasename() {
        String completeFileName = "/some/file.with.multiple.extensions";
        assertThat(GridssCommand.basenameNoExtensions(completeFileName)).isEqualTo("file");
    }

    @Test
    public void shouldReturnFilenameUnchangedWhenItIsAlreadyInBasenameFormat() {
        String basename = "filename";
        assertThat(GridssCommand.basenameNoExtensions(basename)).isEqualTo(basename);
    }
}