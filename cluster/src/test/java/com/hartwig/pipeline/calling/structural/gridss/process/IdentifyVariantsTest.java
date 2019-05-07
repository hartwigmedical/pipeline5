package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.TestConstants;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class IdentifyVariantsTest {
    private static final String CLASSNAME = "gridss.IdentifyVariants";
    private static final String OUTPUT_FILE = String.format("%s/sv_calling.vcf", TestConstants.OUT_DIR);

    private IdentifyVariants command;
    private String sampleBam;
    private String tumorBam;
    private String assemblyBam;
    private String referenceGenome;
    private String blacklist;

    @Before
    public void setup() {
        sampleBam = "/sample.bam";
        tumorBam = "/tumor.bam";
        assemblyBam = "/assembly.bam";
        referenceGenome = TestConstants.REF_GENOME;
        blacklist = "/blacklist.bed";
        command = new IdentifyVariants(sampleBam, tumorBam, assemblyBam, referenceGenome, blacklist);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassname() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(
                asList("-Dgridss.output_to_temp_file=true"), CLASSNAME, "8G");
    }

    @Test
    public void shouldEndCommandLineWithGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments("tmp_dir", "/tmp")
                .and("working_dir", TestConstants.OUT_DIR)
                .and("reference_sequence", referenceGenome)
                .and("input", sampleBam)
                .and("input", tumorBam)
                .and("output_vcf", OUTPUT_FILE)
                .and("assembly", assemblyBam)
                .and("worker_threads", "16")
                .and("blacklist", blacklist)
                .and("configuration_file", TestConstants.GRIDSS_CONFIG)
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(CLASSNAME);
    }

    @Test
    public void shouldReturnOutputVcf() {
        assertThat(command.resultantVcf()).isNotNull();
        assertThat(command.resultantVcf()).isEqualTo(OUTPUT_FILE);
    }
}