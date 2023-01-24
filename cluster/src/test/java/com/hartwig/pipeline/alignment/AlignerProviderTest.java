package com.hartwig.pipeline.alignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class AlignerProviderTest {

    private static final Arguments LOCAL_ARGUMENTS = Arguments.testDefaults();
    private GoogleCredentials credentials;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        credentials = mock(GoogleCredentials.class);
        storage = mock(Storage.class);
    }

    @Test
    public void wiresUpAlignerWithLocalDependencies() throws Exception {
        AlignerProvider victim =
                AlignerProvider.from(TestInputs.pipelineInput(), credentials, storage, LOCAL_ARGUMENTS, mock(Labels.class));
        assertThat(victim.get()).isNotNull();
        assertThat(victim).isInstanceOf(AlignerProvider.LocalAlignerProvider.class);
    }
}
