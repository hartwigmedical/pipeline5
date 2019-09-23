package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.resource.ResourceLocation;
import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.RESOURCE_DIR;
import static com.hartwig.pipeline.testsupport.TestConstants.resource;
import static org.assertj.core.api.Assertions.assertThat;

public class ResourceDownloadTest {

    private ResourceDownload victim;

    @Before
    public void setUp() throws Exception {
        victim = new ResourceDownload(ResourceLocation.builder().bucket("runtime/bucket").addFiles("path/file1.ext1", "path/file2.ext2").build());
    }

    @Test
    public void createsBashToDownloadAllResourceFilesWithGsUtil() {
        assertThat(victim.asBash()).isEqualTo("gsutil -qm cp gs://runtime/bucket/* " + RESOURCE_DIR);
    }

    @Test
    public void keepsLocalFilePathsAsStateForReference() {
        assertThat(victim.getLocalPaths()).containsExactly(resource("file1.ext1"), resource("file2.ext2"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void findThrowsIllegalArgumentExceptionWhenExtensionNotInFiles() {
        victim.find("ext3");
    }

    @Test
    public void findsFirstFileWithExtensionMatch() {
        assertThat(victim.find("ext2", "ext3")).isEqualTo(resource("file2.ext2"));
    }
}