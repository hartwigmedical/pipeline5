package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import org.junit.Before;
import org.junit.Test;

public class AnnotateVariantsTest implements CommonEntities {
    private AnnotateVariants command;
    private String assemblyBam;
    private String inputVcf;
    private String expectedResultantVcf;
    private String configurationFile;
    private String blacklist;

    @Before
    public void setup() {
        assemblyBam = "assembly.bam";
        configurationFile = "/some/stuff.props";
        blacklist = "/the/blacklist";
        inputVcf = "input.vcf";
        expectedResultantVcf = format("%s/sample12345678R_sample12345678T.annotated_variants.vcf", OUT_DIR);
        command = new AnnotateVariants(REFERENCE_BAM, TUMOR_BAM, assemblyBam, inputVcf, REFERENCE_GENOME, JOINT_NAME, configurationFile, blacklist);
    }

    @Test
    public void shouldReturnClassname() {
        assertThat(command.className()).isEqualTo("gridss.AnnotateVariants");
    }


    @Test
    public void shouldUseGridssStandardHeapSize() {
        GridssCommonArgumentsAssert.assertThat(command).usesStandardAmountOfMemory();
    }

    @Test
    public void shouldReturnResultantVcf() {
        assertThat(command.resultantVcf()).isEqualTo(expectedResultantVcf);
    }

    @Test
    public void shouldConstructGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments(ARGS_TMP_DIR)
                .and("working_dir", VmDirectories.OUTPUT)
                .and("reference_sequence", REFERENCE_GENOME)
                .and("input", REFERENCE_BAM)
                .and("input", TUMOR_BAM)
                .and("input_vcf", inputVcf)
                .and("output_vcf", expectedResultantVcf)
                .and("assembly", assemblyBam)
                .andBlacklist(blacklist)
                .andConfigFile(configurationFile)
                .andNoMore();
    }
}