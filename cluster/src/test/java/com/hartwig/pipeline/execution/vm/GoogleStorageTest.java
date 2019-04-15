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
    private GoogleStorage storage;

    @Before
    public void setup() {
        bucket = randomStr();
        remote = randomStr();
        local = randomStr();

        storage = new GoogleStorage(bucket);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfNullBucketNameIsProvidedToConstructor() {
        new GoogleStorage(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfOnlyWhitespaceIsProvidedToConstructor() {
        new GoogleStorage("  ");
    }

    @Test
    public void shouldGenerateCopyToLocalCommandUsingGivenBucketNameAndProvidedLocalAndRemotePaths() {
        String command = storage.copyToLocal(remote, local);
        assertThat(command).isEqualTo(format("gsutil -q cp gs://%s/%s %s", bucket, remote, local));
    }

    @Test
    public void shouldGenerateBucketCreationCommand() {
        assertThat(storage.create()).isEqualTo(format("gsutil mb -l europe-west4 gs://%s", bucket));
    }

    @Test
    public void shouldGenerateCopyFromLocalCommandUsingGivenBucketNameAndProvidedLocalAndRemotePaths() {
        String command = storage.copyFromLocal(local, remote);
        assertThat(command).isEqualTo(format("gsutil -q cp %s gs://%s/%s", local, bucket, remote));
    }
}