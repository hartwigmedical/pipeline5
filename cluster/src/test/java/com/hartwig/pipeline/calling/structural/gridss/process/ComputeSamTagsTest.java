package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.TestConstants;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class ComputeSamTagsTest {
    private ComputeSamTags command;
    private final String CLASSNAME = "gridss.ComputeSamTags";
    private final String OUTFILE = format("%s/compute_sam_tags.bam", VmDirectories.OUTPUT);
    private String inputBam;

    @Before
    public void setup() {
        inputBam = "/input.bam";
        command = new ComputeSamTags(inputBam, TestConstants.REF_GENOME);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassName() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(CLASSNAME, "4G");
    }

    @Test
    public void shouldCompleteCommandLineWithGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments("tmp_dir", "/tmp")
                .and("working_dir", TestConstants.OUT_DIR)
                .and("reference_sequence", TestConstants.REF_GENOME)
                .and("compression_level", "0")
                .and("i", inputBam)
                .and("o", "/dev/stdout")
                .and("recalculate_sa_supplementary", "true")
                .and("soften_hard_clips", "true")
                .and("fix_mate_information", "true")
                .and("fix_duplicate_flag", "true")
                .and("tags", "null")
                .and("tags", "NM")
                .and("tags", "SA")
                .and("tags", "R2")
                .and("tags", "Q2")
                .and("tags", "MC")
                .and("tags", "MQ")
                .and("assume_sorted", format("true | samtools sort -O bam -T /tmp/samtools.sort.tmp -@ 2 -o %s", OUTFILE))
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(CLASSNAME);
    }

    @Test
    public void shouldReturnResultantBam() {
        assertThat(command.resultantBam()).isNotNull();
        assertThat(command.resultantBam()).isEqualTo(OUTFILE);
    }
}
