package com.hartwig.pipeline.calling.germline;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class SelectVariantsTest extends SubStageTest{

    @Override
    public SubStage createVictim() {
        return new SelectVariants("snp", Lists.newArrayList("SNP,NO_VARIATION"), "referenceSampleName.fasta");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.raw_snp.vcf";
    }

    @Test
    public void selectsVariantsWithGatk() {
        assertThat(bash()).contains("java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T "
                + "SelectVariants -selectType SNP,NO_VARIATION -R referenceSampleName.fasta -V "
                + "/data/output/tumor.strelka.vcf -o /data/output/tumor.raw_snp.vcf");
    }
}