package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class IdentifyVariantsTest implements CommonEntities {
    private static final String OUTPUT_FILE = String.format("%s/sv_calling.vcf", OUT_DIR);

    private IdentifyVariants command;
    private String assemblyBam;
    private String className = "gridss.IdentifyVariants";

    @Before
    public void setup() {
        assemblyBam = "/assembly.bam";
        command = new IdentifyVariants(REFERENCE_BAM, TUMOR_BAM, assemblyBam, REFERENCE_GENOME);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassname() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(
                asList("-Dgridss.output_to_temp_file=true"), className, "8G");
    }

    @Test
    public void shouldEndCommandLineWithGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments(ARGS_TMP_DIR)
                .and("working_dir", OUT_DIR)
                .and(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT, REFERENCE_BAM)
                .and(ARG_KEY_INPUT, TUMOR_BAM)
                .and("output_vcf", OUTPUT_FILE)
                .and("assembly", assemblyBam)
                .and(ARG_KEY_WORKER_THREADS, "16")
                .and(ARGS_BLACKLIST)
                .and(ARGS_GRIDSS_CONFIG)
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(className);
    }

    @Test
    public void shouldReturnOutputVcf() {
        assertThat(command.resultantVcf()).isNotNull();
        assertThat(command.resultantVcf()).isEqualTo(OUTPUT_FILE);
    }
}