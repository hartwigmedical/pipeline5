package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_GATK_JAR;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class GenotypeGVCFsTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new GenotypeGVCFs("reference.fasta", "dbsnps.vcf");
    }

    @Override
    public String expectedPath() {
        return outFile("reference.genotype_vcfs.vcf");
    }

    @Test
    public void runsGatkGenotypeGvcfs() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx20G -jar " + TOOLS_GATK_JAR + " -T "
                + "GenotypeGVCFs -V " + outFile("reference.strelka.vcf") + " -R reference.fasta -D dbsnps.vcf -o "
                + expectedPath());
    }

    @Override
    protected String sampleName() {
        return "reference";
    }
}