package com.hartwig.pipeline.calling.germline;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

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
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T "
                + "GenotypeGVCFs -V " + outFile("reference.strelka.vcf") + " -R reference.fasta -D dbsnps.vcf -o "
                + expectedPath());
    }

    @Override
    protected String sampleName() {
        return "reference";
    }
}