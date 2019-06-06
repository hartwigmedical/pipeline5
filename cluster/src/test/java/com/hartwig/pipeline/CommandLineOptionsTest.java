package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.junit.Test;

public class CommandLineOptionsTest {

    private static final String OVERRIDDEN_SAMPLE_DIR = "/sample/dir";

    @Test
    public void createDefaultOptionsForDevelopment() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "development", "-sample_id", "test" });
        assertThat(result.profile()).isEqualTo(Arguments.DefaultsProfile.DEVELOPMENT);
    }

    @Test
    public void createDefaultOptionsForProduction() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "production", "-sample_id", "test" });
        assertThat(result.profile()).isEqualTo(Arguments.DefaultsProfile.PRODUCTION);
    }

    @Test(expected = RuntimeException.class)
    public void unknownProfileTypeThrowsException() throws Exception {
        CommandLineOptions.from(new String[] { "-profile", "unknown", "-sample_id", "test" });
    }

    @Test(expected = MissingOptionException.class)
    public void missingSampleIdThrowsException() throws Exception {
        CommandLineOptions.from(new String[] { "-profile", "development" });
    }

    @Test(expected = UnrecognizedOptionException.class)
    public void invalidArgumentThrowsException() throws Exception {
        CommandLineOptions.from(new String[] { "-not_an_arg", "nonsense", "-sample_id", "test" });
    }

    @Test
    public void defaultOptionsCanBeOveridden() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "development", "-sample_id", "test", "-sample_directory",
                OVERRIDDEN_SAMPLE_DIR });
        assertThat(result.sampleDirectory()).isEqualTo(OVERRIDDEN_SAMPLE_DIR);
    }

    @Test
    public void booleanFlagDefaultsRespected() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "development", "-sample_id", "test" });
        assertThat(result.download()).isFalse();
    }

    @Test
    public void defaultFlagsCanBeOveriddenTrue() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "development", "-sample_id", "test", "-upload", "true" });
        assertThat(result.upload()).isTrue();
    }

    @Test
    public void defaultFlagsCanBeOveriddenFalse() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "development", "-sample_id", "test", "-cleanup", "false" });
        assertThat(result.cleanup()).isFalse();
    }

    @Test(expected = ParseException.class)
    public void nonBooleanValuesForFlagsHandled() throws Exception {
        CommandLineOptions.from(new String[] { "-profile", "development", "-sample_id", "test", "-cleanup", "notboolean" });
    }
}