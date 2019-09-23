package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.GridssTestConstants;
import com.hartwig.pipeline.testsupport.TestConstants;
import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.calling.structural.gridss.GridssTestConstants.REFERENCE_GENOME;
import static com.hartwig.pipeline.testsupport.TestConstants.OUT_DIR;
import static org.assertj.core.api.Assertions.assertThat;

public class IdentifyVariantsTest {
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
        outputVcf = TestConstants.outFile("sv_calling.vcf");
        className = "gridss.IdentifyVariants";
        command = new IdentifyVariants(GridssTestConstants.REFERENCE_BAM, GridssTestConstants.TUMOR_BAM, assemblyBam, outputVcf, REFERENCE_GENOME, configurationFile, blacklist);
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
                .hasGridssArguments(GridssTestConstants.ARGS_TMP_DIR)
                .and("working_dir", OUT_DIR)
                .and(GridssTestConstants.ARGS_REFERENCE_SEQUENCE)
                .and(GridssTestConstants.ARG_KEY_INPUT, GridssTestConstants.REFERENCE_BAM)
                .and(GridssTestConstants.ARG_KEY_INPUT, GridssTestConstants.TUMOR_BAM)
                .and("output_vcf", outputVcf)
                .and("assembly", assemblyBam)
                .andBlacklist(blacklist)
                .andConfigFile(configurationFile)
                .andNoMore();
    }
}