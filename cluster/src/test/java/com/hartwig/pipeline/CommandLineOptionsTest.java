package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.junit.Test;

public class CommandLineOptionsTest {

    private static final String OVERRIDDEN_SET_ID = "set";

    @Test
    public void createDefaultOptionsForDevelopment() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "development", "-set_id", "test" });
        assertThat(result.profile()).isEqualTo(Arguments.DefaultsProfile.DEVELOPMENT);
    }

    @Test
    public void createDefaultOptionsForProduction() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "production", "-set_id", "test" });
        assertThat(result.profile()).isEqualTo(Arguments.DefaultsProfile.PRODUCTION);
    }

    @Test(expected = RuntimeException.class)
    public void unknownProfileTypeThrowsException() throws Exception {
        CommandLineOptions.from(new String[] { "-profile", "unknown", "-set_id", "test" });
    }

    @Test(expected = UnrecognizedOptionException.class)
    public void invalidArgumentThrowsException() throws Exception {
        CommandLineOptions.from(new String[] { "-not_an_arg", "nonsense", "-set_id", "test" });
    }

    @Test
    public void defaultOptionsCanBeOverridden() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "development", "-set_id", OVERRIDDEN_SET_ID });
        assertThat(result.setName()).isEqualTo(OVERRIDDEN_SET_ID);
    }

    @Test
    public void booleanFlagDefaultsRespected() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "development", "-set_id", "test" });
        assertThat(result.useLocalSsds()).isTrue();
    }

    @Test
    public void defaultFlagsCanBeOverriddenTrue() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "development", "-set_id", "test", "-local_ssds", "false" });
        assertThat(result.useLocalSsds()).isFalse();
    }

    @Test
    public void defaultFlagsCanBeOverriddenFalse() throws Exception {
        Arguments result = CommandLineOptions.from(new String[] { "-profile", "development", "-set_id", "test", "-cleanup", "false" });
        assertThat(result.cleanup()).isFalse();
    }

    @Test(expected = ParseException.class)
    public void nonBooleanValuesForFlagsHandled() throws Exception {
        CommandLineOptions.from(new String[] { "-profile", "development", "-set_id", "test", "-cleanup", "notboolean" });
    }
}