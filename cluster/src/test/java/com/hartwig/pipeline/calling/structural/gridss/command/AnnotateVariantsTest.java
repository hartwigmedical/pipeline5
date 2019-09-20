package com.hartwig.pipeline.calling.structural.gridss.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.GridssTestEntities;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import org.junit.Before;
import org.junit.Test;

public class AnnotateVariantsTest implements GridssTestEntities {
    private AnnotateVariants command;
    private String assemblyBam;
    private String inputVcf;
    private String configurationFile;
    private String blacklist;
    private String className;
    private String outputVcf;

    @Before
    public void setup() {
        assemblyBam = "assembly.bam";
        configurationFile = "/some/stuff.props";
        blacklist = "/the/blacklist";
        inputVcf = "input.vcf";
        outputVcf = "/some/path/output_file.vcf";
        className = "gridss.AnnotateVariants";
        command = new AnnotateVariants(REFERENCE_BAM, TUMOR_BAM, assemblyBam, inputVcf, REFERENCE_GENOME, outputVcf, configurationFile, blacklist);
    }

    @Test
    public void shouldGenerateCorrectJavaArguments() {
        GridssCommonArgumentsAssert.assertThat(command).generatesJavaInvocationUpToAndIncludingClassname(className);
    }

    @Test
    public void shouldReturnClassname() {
        assertThat(command.className()).isEqualTo(className);
    }

    @Test
    public void shouldUseGridssStandardHeapSize() {
        GridssCommonArgumentsAssert.assertThat(command).usesStandardAmountOfMemory();
    }

    @Test
    public void shouldConstructGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments(ARGS_TMP_DIR)
                .and("working_dir", VmDirectories.OUTPUT)
                .and("reference_sequence", REFERENCE_GENOME)
                .and("input", REFERENCE_BAM)
                .and("input", TUMOR_BAM)
                .and("input_vcf", inputVcf)
                .and("output_vcf", outputVcf)
                .and("assembly", assemblyBam)
                .andBlacklist(blacklist)
                .andConfigFile(configurationFile)
                .andNoMore();
    }
}