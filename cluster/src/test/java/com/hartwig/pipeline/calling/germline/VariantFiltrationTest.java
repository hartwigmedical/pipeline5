package com.hartwig.pipeline.calling.germline;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class VariantFiltrationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new VariantFiltration("snps", ImmutableMap.of("filter1", "QD < 2.0", "filter2", "MQ < 10.0"), "reference.fasta");
    }

    @Override
    public String expectedPath() {
        return "/data/output/reference.filtered_snps.vcf";
    }

    @Test
    public void filtersVariantsWithGatk() {
        assertThat(bash()).contains("java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T "
                + "VariantFiltration -R reference.fasta -V /data/output/reference.strelka.vcf -o "
                + "/data/output/reference.filtered_snps.vcf --filterExpression \"QD < 2.0\" --filterName \"filter1\" --filterExpression "
                + "\"MQ < 10.0\" --filterName \"filter2\" --clusterSize 3 --clusterWindowSize 35");
    }

    @Override
    protected String sampleName() {
        return "reference";
    }
}