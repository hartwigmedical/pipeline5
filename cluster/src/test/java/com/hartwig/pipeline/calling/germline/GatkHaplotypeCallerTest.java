package com.hartwig.pipeline.calling.germline;

import org.junit.Test;

import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;

public class GatkHaplotypeCallerTest {
    @Test
    public void shouldGenerateCorrectCommandFromArguments() {
        String jar = randStr();
        String inputBam = randStr();
        String referenceFasta = randStr();
        String outputVcf = randStr();

        String expected = "java -cp " + jar + " org.broadinstitute.hellbender.Main HaplotypeCaller" + " --input " + inputBam + " --output "
                + outputVcf + " --reference " + referenceFasta;

        assertThat(new GatkHaplotypeCaller(jar, inputBam, referenceFasta, outputVcf).buildCommand()).isEqualTo(expected);
    }

    private static String randStr() {
        return randomAlphabetic(16);
    }
}