package com.hartwig.pipeline.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class ArchivePathTest {

    @Test
    public void pathWithRootFolder() {
        assertThat(new ArchivePath(Folder.root(), "namespace", "filename").path()).isEqualTo("namespace/filename");
    }

    @Test
    public void pathWithSingleSampleFolder() {
        assertThat(new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), "namespace", "filename").path()).isEqualTo(
                "reference/namespace/filename");
    }
}