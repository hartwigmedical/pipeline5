package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.calling.structural.gridss.command.BiocondaVariantAnnotationWorkaround;
import com.hartwig.pipeline.calling.structural.gridss.command.RscriptFilter;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import org.junit.Test;

public class FilterTest extends SubStageTest {

    private static final String UNZIPPED = "/data/output/tumor.gridss.vcf";
    private static final String OUTPUT_FULL_VCF = "full.vcf";
    private static final String OUTPUT_ORIGINAL_VCF = VmDirectories.outputFile("original.vcf");

    @Override
    public SubStage createVictim() {
        return new Filter(OUTPUT_ORIGINAL_VCF, OUTPUT_FULL_VCF);
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.filter.vcf.gz";
    }

    @Override
    protected OutputFile input() {
        return OutputFile.of(sampleName(), "gridss", OutputFile.GZIPPED_VCF, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfInputPathDoesNotEndWithGz() {
        createVictim().apply(SubStageInputOutput.of("nogz", OutputFile.of("nogz", OutputFile.VCF), Collections.emptyList()));
    }

    @Test
    public void shouldRunBiocondaWorkaroundAsFirstStepPassingDeducedUncompressedFilenameAsOutput() {
        BiocondaVariantAnnotationWorkaround command = new BiocondaVariantAnnotationWorkaround(input().path(), UNZIPPED);
        assertThat(output.bash().get(0).asBash()).contains(format("(%s)", command.asBash()));
    }

    @Test
    public void shouldRunRscriptWithCorrectScriptAsSecondStep() {
        String expectedRscript = new RscriptFilter(UNZIPPED, "/data/output/original.vcf", "full.vcf").asBash();
        assertThat(output.bash().get(1).asBash()).isEqualTo(expectedRscript);
    }

    @Test
    public void shouldMoveInterimFullVcfAndTbiToFinalLocationAfterRscriptRuns() {
        assertThat(output.bash().get(2).asBash()).isEqualTo(format("mv %s.bgz %s.gz", OUTPUT_FULL_VCF, OUTPUT_FULL_VCF));
        assertThat(output.bash().get(3).asBash()).isEqualTo(format("mv %s.bgz.tbi %s.gz.tbi", OUTPUT_FULL_VCF, OUTPUT_FULL_VCF));
    }

    @Test
    public void shouldMoveInterimFilteredVcfAndTbiToFinalLocation() {
        assertThat(output.bash().get(4).asBash()).isEqualTo(format("mv %s.bgz %s.gz", OUTPUT_ORIGINAL_VCF, OUTPUT_ORIGINAL_VCF));
        assertThat(output.bash().get(5).asBash()).isEqualTo(format("mv %s.bgz.tbi %s.gz.tbi", OUTPUT_ORIGINAL_VCF, OUTPUT_ORIGINAL_VCF));
    }
}