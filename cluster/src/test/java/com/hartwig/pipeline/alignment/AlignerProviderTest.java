package com.hartwig.pipeline.alignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.pipeline.Arguments;

import org.junit.Before;
import org.junit.Test;

public class AlignerProviderTest {

    private static final Arguments LOCAL_ARGUMENTS = Arguments.testDefaults();
    private CredentialProvider credentialProvider;

    @Before
    public void setUp() throws Exception {
        credentialProvider = mock(CredentialProvider.class);
        when(credentialProvider.get()).thenReturn(mock(GoogleCredentials.class));
    }

    @Test
    public void wiresUpBootstrapWithLocalDependencies() throws Exception {
        AlignerProvider victim = AlignerProvider.from(LOCAL_ARGUMENTS, credentialProvider);
        assertThat(victim.get()).isNotNull();
        assertThat(victim).isInstanceOf(AlignerProvider.LocalBootstrapProvider.class);
    }

    @Test
    public void wiresUpBootstrapWithSbpDependencies() throws Exception {
        AlignerProvider victim = AlignerProvider.from(Arguments.testDefaultsBuilder().sbpApiSampleId(1).build(), credentialProvider);
        assertThat(victim.get()).isNotNull();
        assertThat(victim).isInstanceOf(AlignerProvider.SBPBootstrapProvider.class);
    }
}