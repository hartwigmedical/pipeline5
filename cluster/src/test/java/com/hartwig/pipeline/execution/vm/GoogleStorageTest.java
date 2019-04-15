package com.hartwig.pipeline.execution.vm;

import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.execution.vm.DataFixture.randomStr;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class GoogleStorageTest {
    private String bucket;
    private String remote;
    private String local;
    private GoogleStorageInputOutput storage;

    @Before
    public void setup() {
        bucket = randomStr();
        remote = randomStr();
        local = randomStr();

        storage = new GoogleStorageInputOutput(bucket);
    }

    @Test
    public void shouldGenerateCopyToLocalCommandUsingGivenBucketNameAndProvidedLocalAndRemotePaths() {
        String command = storage.copyToLocal(remote, local);
        assertThat(command).isEqualTo(format("gsutil -qm cp gs://%s/%s %s", bucket, remote, local));
    }

    @Test
    public void shouldGenerateCopyFromLocalCommandUsingGivenBucketNameAndProvidedLocalAndRemotePaths() {
        String command = storage.copyFromLocal(local, remote);
        assertThat(command).isEqualTo(format("gsutil -qm cp %s gs://%s/%s", local, bucket, remote));
    }
}