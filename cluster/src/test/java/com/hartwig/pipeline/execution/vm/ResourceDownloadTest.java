package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.resource.ResourceLocation;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;

public class ResourceDownloadTest {

    private ResourceDownload victim;

    @Before
    public void setUp() throws Exception {
        victim = new ResourceDownload(ResourceLocation.builder().bucket("bucket").addFiles("path/file1", "path/file2").build(),
                MockRuntimeBucket.of("runtime").getRuntimeBucket());
    }

    @Test
    public void createsBashToDownloadAllResourceFilesWithGsUtil() {
        assertThat(victim.asBash()).isEqualTo("gsutil -m cp gs://runtime/bucket/* /data/resources");
    }

    @Test
    public void keepsLocalFilePathsAsStateForReference() {
        assertThat(victim.getLocalPaths()).containsExactly("/data/resources/file1", "/data/resources/file2");
    }
}