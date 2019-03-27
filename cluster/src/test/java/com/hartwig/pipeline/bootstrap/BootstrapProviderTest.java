package com.hartwig.pipeline.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.GoogleCredentials;

import org.junit.Before;
import org.junit.Test;

public class BootstrapProviderTest {

    private static final Arguments LOCAL_ARGUMENTS = Arguments.defaults();
    private CredentialProvider credentialProvider;

    @Before
    public void setUp() throws Exception {
        credentialProvider = mock(CredentialProvider.class);
        when(credentialProvider.get()).thenReturn(mock(GoogleCredentials.class));
    }

    @Test
    public void wiresUpBootstrapWithLocalDependencies() throws Exception {
        BootstrapProvider victim = BootstrapProvider.from(LOCAL_ARGUMENTS, credentialProvider);
        assertThat(victim.get()).isNotNull();
        assertThat(victim).isInstanceOf(BootstrapProvider.LocalBootstrapProvider.class);
    }

    @Test
    public void wiresUpBootstrapWithSbpDependencies() throws Exception {
        BootstrapProvider victim = BootstrapProvider.from(Arguments.defaultsBuilder().sbpApiSampleId(1).build(), credentialProvider);
        assertThat(victim.get()).isNotNull();
        assertThat(victim).isInstanceOf(BootstrapProvider.SBPBootstrapProvider.class);
    }
}