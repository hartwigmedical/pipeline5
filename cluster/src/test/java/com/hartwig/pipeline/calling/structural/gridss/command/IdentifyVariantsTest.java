package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;

import org.junit.Before;
import org.junit.Test;

public class IdentifyVariantsTest implements CommonEntities {
    private IdentifyVariants command;
    private String assemblyBam;
    private String outputVcf;
    private String configurationFile;
    private String blacklist;
    private String className;

    @Before
    public void setup() {
        assemblyBam = "/assembly.bam";
        configurationFile = "/config.properties";
        blacklist = "/path/to/blacklist.bed";
        outputVcf =  format("%s/sv_calling.vcf", OUT_DIR);
        className = "gridss.IdentifyVariants";
        command = new IdentifyVariants(REFERENCE_BAM, TUMOR_BAM, assemblyBam, outputVcf, REFERENCE_GENOME, configurationFile, blacklist);
    }

    @Test
    public void shouldGenerateCorrectJavaArguments() {
        GridssCommonArgumentsAssert.assertThat(command).generatesJavaInvocationUpToAndIncludingClassname(className);
    }

    @Test
    public void shouldReturnClassName() {
        assertThat(command.className()).isEqualTo(className);
    }

    @Test
    public void shouldUseStandardAmountOfMemory() {
        GridssCommonArgumentsAssert.assertThat(command).usesStandardAmountOfMemory();
    }

    @Test
    public void shouldReturnGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments(ARGS_TMP_DIR)
                .and("working_dir", OUT_DIR)
                .and(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT, REFERENCE_BAM)
                .and(ARG_KEY_INPUT, TUMOR_BAM)
                .and("output_vcf", outputVcf)
                .and("assembly", assemblyBam)
                .andBlacklist(blacklist)
                .andConfigFile(configurationFile)
                .andNoMore();
    }
}