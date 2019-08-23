package com.hartwig.pipeline.calling.germline.command;

import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class GatkHaplotypeCallerCommandTest {
    @Test
    public void shouldGenerateCorrectCommandFromArguments() {
        String inputBam = randStr();
        String referenceFasta = randStr();
        String snpDb = randStr();
        String outputVcf = randStr();

        String expected = "java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T HaplotypeCaller "
                + "-nct $(grep -c '^processor' /proc/cpuinfo) --input_file " + inputBam
                + " -o " + outputVcf + " -D " + snpDb + " --reference_sequence " + referenceFasta
                + " -variant_index_type LINEAR -variant_index_parameter 128000 -stand_call_conf 15.0"
                + " -ERC GVCF -GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60 --sample_ploidy 2";

        assertThat(new GatkHaplotypeCallerCommand(inputBam, referenceFasta, snpDb, outputVcf).asBash()).isEqualTo(expected);
    }

    private static String randStr() {
        return randomAlphabetic(16);
    }
}