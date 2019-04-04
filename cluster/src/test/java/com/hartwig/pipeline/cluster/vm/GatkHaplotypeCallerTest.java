package com.hartwig.pipeline.cluster.vm;

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

        String expected = new StringBuilder("java -cp ")
                .append(jar)
                .append(" org.broadinstitute.hellbender.Main HaplotypeCaller")
                .append(" --input ")
                .append(inputBam)
                .append(" --output ")
                .append(outputVcf)
                .append(" --reference ")
                .append(referenceFasta)
                .toString();

        assertThat(new GatkHaplotypeCaller(jar, inputBam, referenceFasta, outputVcf).buildCommand()).isEqualTo(expected);
    }

    private static String randStr() {
        return randomAlphabetic(16);
    }
}