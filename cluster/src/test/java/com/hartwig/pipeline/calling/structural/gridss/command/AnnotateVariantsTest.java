package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class AnnotateVariantsTest implements CommonEntities {
    private AnnotateVariants command;
    private String assemblyBam;
    private String inputVcf;
    private String expectedResultantVcf;

    @Before
    public void setup() {
        assemblyBam = "assembly.bam";
        inputVcf = "input.vcf";
        expectedResultantVcf = format("%s/annotate_variants.vcf", OUT_DIR);
        command = new AnnotateVariants(REFERENCE_BAM, TUMOR_BAM, assemblyBam, inputVcf, REFERENCE_GENOME);
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
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments("tmp_dir", "/tmp")
                .and("working_dir", VmDirectories.OUTPUT)
                .and("reference_sequence", REFERENCE_GENOME)
                .and("input", REFERENCE_BAM)
                .and("input", TUMOR_BAM)
                .and("input_vcf", inputVcf)
                .and("output_vcf", expectedResultantVcf)
                .and("assembly", assemblyBam)
                .and("worker_threads", "2")
                .and("blacklist", GridssCommon.blacklist())
                .and("configuration_file", GridssCommon.configFile())
                .andNoMore();
    }
}