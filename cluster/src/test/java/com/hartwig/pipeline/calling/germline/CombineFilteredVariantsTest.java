package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_GATK_JAR;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class CombineFilteredVariantsTest extends SubStageTest{

    @Override
    public SubStage createVictim() {
        return new CombineFilteredVariants("other.vcf", "referenceSampleName.fasta");
    }

    @Override
    public String expectedPath() {
        return outFile("referenceSampleName.filtered_variants.vcf");
    }

    @Test
    public void combinesVariantsWithGatk() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx20G -jar " + TOOLS_GATK_JAR + " -T "
                + "CombineVariants -V " + outFile("referenceSampleName.strelka.vcf") + " -V other.vcf -o "
                + expectedPath() + " -R referenceSampleName.fasta --assumeIdenticalSamples");
    }

    @Override
    protected String sampleName() {
        return "referenceSampleName";
    }
}