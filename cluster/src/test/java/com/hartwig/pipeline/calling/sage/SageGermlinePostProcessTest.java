package com.hartwig.pipeline.calling.sage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class SageGermlinePostProcessTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new SageGermlinePostProcess("reference", "tumor", TestInputs.REF_GENOME_37_RESOURCE_FILES);
    }

    @Override
    protected OutputFile input() {
        return OutputFile.of(sampleName(), "sage.germline", "vcf.gz");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.sage.germline.filtered.vcf.gz";
    }

    @Test
    public void testBashCommands() {
        assertThat(output.bash().stream().map(BashCommand::asBash).collect(Collectors.toList())).isEqualTo(expectedCommands());
    }

    protected List<String> expectedCommands() {
        return ImmutableList.of(
                "(/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER=\"PASS\"' /data/output/tumor.sage.germline.vcf.gz -O z -o /data/output/tumor.sage.pass.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pass.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools view -s reference,tumor /data/output/tumor.sage.pass.vcf.gz -O z -o /data/output/tumor.sage.sort.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.sort.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/mappability/37/out_150_37.mappability.bed.gz -h /opt/resources/mappability/mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY /data/output/tumor.sage.sort.vcf.gz -O z -o /data/output/tumor.mappability.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.mappability.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/clinvar.37.vcf.gz -c INFO/CLNSIG,INFO/CLNSIGCONF /data/output/tumor.mappability.annotated.vcf.gz -O z -o /data/output/tumor.clinvar.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.clinvar.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/KnownBlacklist.germline.37.bed.gz -m BLACKLIST_BED -c CHROM,FROM,TO /data/output/tumor.clinvar.vcf.gz -O z -o /data/output/tumor.blacklist.regions.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.blacklist.regions.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/KnownBlacklist.germline.37.vcf.gz -m BLACKLIST_VCF /data/output/tumor.blacklist.regions.vcf.gz -O z -o /data/output/tumor.blacklist.variants.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.blacklist.variants.vcf.gz -p vcf",
                "/opt/tools/snpEff/4.3s/snpEff.sh /opt/tools/snpEff/4.3s/snpEff.jar /opt/resources/snpeff/37/snpEff.config GRCh37.75 /data/output/tumor.blacklist.variants.vcf.gz /data/output/tumor.sage.germline.filtered.vcf",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/tumor.sage.germline.filtered.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.germline.filtered.vcf.gz -p vcf");
    }
}