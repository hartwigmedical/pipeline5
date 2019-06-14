package com.hartwig.pipeline.calling.structural.gridss;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GridssCommonTest {
    @Test
    public void shouldExtractBasenameForFileDroppingFinalExtension() {
        String completeFilename = "/some/file/somewhere/with.extension";
        assertThat(GridssCommon.basenameNoExtensions(completeFilename)).isEqualTo("with");
    }

    @Test
    public void shouldNotChokeWhenAskedToDropExtensionsFromBasenameIfThereAreNone() {
        String completeFileName = "/some/filename";
        assertThat(GridssCommon.basenameNoExtensions(completeFileName)).isEqualTo("filename");
    }

    @Test
    public void shouldDropMultipleExtensionsFromBasename() {
        String completeFileName = "/some/file.with.multiple.extensions";
        assertThat(GridssCommon.basenameNoExtensions(completeFileName)).isEqualTo("file");
    }

    @Test
    public void shouldReturnFilenameUnchangedWhenItIsAlreadyInBasenameFormat() {
        String basename = "filename";
        assertThat(GridssCommon.basenameNoExtensions(basename)).isEqualTo(basename);
    }
}