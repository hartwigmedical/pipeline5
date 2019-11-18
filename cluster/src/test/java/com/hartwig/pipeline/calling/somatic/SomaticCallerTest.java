package com.hartwig.pipeline.calling.somatic;

import static com.hartwig.pipeline.resource.ResourceNames.BEDS;
import static com.hartwig.pipeline.resource.ResourceNames.COSMIC;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.PON;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;
import static com.hartwig.pipeline.resource.ResourceNames.STRELKA_CONFIG;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class SomaticCallerTest extends TertiaryStageTest<SomaticCallerOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockResource.addToStorage(storage, REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, SAGE, "hotspots.tsv", "coding_regions.bed", "SAGE_PON.vcf.gz");
        MockResource.addToStorage(storage, STRELKA_CONFIG, "strelka.ini");
        MockResource.addToStorage(storage, MAPPABILITY, "mappability.bed.gz", "mappability.hdr");
        MockResource.addToStorage(storage, PON, "GERMLINE_PON.vcf.gz", "SOMATIC_PON.vcf.gz");
        MockResource.addToStorage(storage, BEDS, "strelka-post-process.bed");
        MockResource.addToStorage(storage, SNPEFF, "snpeff.config", "snpeffdb.zip");
        MockResource.addToStorage(storage, COSMIC, "cosmic_collapsed.vcf.gz");
    }

    @Override
    protected Stage<SomaticCallerOutput, SomaticRunMetadata> createVictim() {
        return new SomaticCaller(TestInputs.defaultPair());
    }

    @Override
    protected List<String> expectedResources() {
        return ImmutableList.of(resource(REFERENCE_GENOME),
                resource(STRELKA_CONFIG),
                resource(MAPPABILITY),
                resource(PON),
                resource(BEDS),
                resource(SAGE),
                resource(SNPEFF),
                resource(COSMIC));
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of(
                "java -Xmx8G -cp $TOOLS_DIR/sage/1.1/sage.jar com.hartwig.hmftools.sage.SageHotspotApplication -tumor tumor -tumor_bam /data/input/tumor.bam -reference reference -reference_bam /data/input/reference.bam -known_hotspots /data/resources/hotspots.tsv -coding_regions /data/resources/coding_regions.bed -ref_genome /data/resources/reference.fasta -out /data/output/tumor.sage.hotspots.vcf.gz",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.sage.hotspots.vcf.gz -p vcf",
                "($TOOLS_DIR/bcftools/1.3.1/bcftools filter -i 'FILTER=\"PASS\"' /data/output/tumor.sage.hotspots.vcf.gz -O u | $TOOLS_DIR/bcftools/1.3.1/bcftools annotate -x INFO/HOTSPOT -O u | $TOOLS_DIR/bcftools/1.3.1/bcftools annotate -x FILTER/LOW_CONFIDENCE -O u | $TOOLS_DIR/bcftools/1.3.1/bcftools annotate -x FILTER/GERMLINE_INDEL -O u | $TOOLS_DIR/bcftools/1.3.1/bcftools view -s tumor -O z -o /data/output/tumor.sage.hotspots.filtered.vcf.gz)",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.sage.hotspots.filtered.vcf.gz -p vcf",
                "$TOOLS_DIR/bcftools/1.3.1/bcftools annotate -a /data/resources/SAGE_PON.vcf.gz -c SAGE_PON_COUNT -o /data/output/tumor.sage.hotspots.pon.annotated.vcf.gz -O z /data/output/tumor.sage.hotspots.filtered.vcf.gz",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.sage.hotspots.pon.annotated.vcf.gz -p vcf",
                "$TOOLS_DIR/bcftools/1.3.1/bcftools filter -e 'SAGE_PON_COUNT!=\".\" && MIN(SAGE_PON_COUNT) > 0' -s SAGE_PON -m+ /data/output/tumor.sage.hotspots.pon.annotated.vcf.gz -O z -o /data/output/tumor.sage.pon.filter.vcf.gz",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.sage.pon.filter.vcf.gz -p vcf",
                "unzip -d /data/resources /data/resources/snpeffdb.zip",
                "$TOOLS_DIR/strelka/1.0.14/bin/configureStrelkaWorkflow.pl --tumor /data/input/tumor.bam --normal /data/input/reference.bam --config /data/resources/strelka.ini --ref /data/resources/reference.fasta --output-dir /data/output/strelkaAnalysis",
                "make -C /data/output/strelkaAnalysis -j $(grep -c '^processor' /proc/cpuinfo)",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T CombineVariants -R /data/resources/reference.fasta --genotypemergeoption unsorted -V:snvs /data/output/strelkaAnalysis/results/passed.somatic.snvs.vcf -V:indels /data/output/strelkaAnalysis/results/passed.somatic.indels.vcf -o /data/output/tumor.strelka.vcf",
                "$TOOLS_DIR/bcftools/1.3.1/bcftools annotate -a /data/resources/mappability.bed.gz -h /data/resources/mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY -o /data/output/tumor.mappability.annotated.vcf.gz -O z /data/output/tumor.strelka.vcf",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.mappability.annotated.vcf.gz -p vcf",
                "$TOOLS_DIR/bcftools/1.3.1/bcftools annotate -a /data/resources/GERMLINE_PON.vcf.gz -c GERMLINE_PON_COUNT -o /data/output/tumor.germline.pon.annotated.vcf.gz -O z /data/output/tumor.mappability.annotated.vcf.gz",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.germline.pon.annotated.vcf.gz -p vcf",
                "$TOOLS_DIR/bcftools/1.3.1/bcftools annotate -a /data/resources/SOMATIC_PON.vcf.gz -c SOMATIC_PON_COUNT -o /data/output/tumor.somatic.pon.annotated.vcf.gz -O z /data/output/tumor.germline.pon.annotated.vcf.gz",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.somatic.pon.annotated.vcf.gz -p vcf",
                "java -Xmx20G -jar $TOOLS_DIR/strelka-post-process/1.6/strelka-post-process.jar -v /data/output/tumor.somatic.pon.annotated.vcf.gz -hc_bed /data/resources/strelka-post-process.bed -t tumor -o /data/output/tumor.strelka.post.processed.vcf.gz -b /data/input/tumor.bam",
                "($TOOLS_DIR/bcftools/1.3.1/bcftools filter -e 'GERMLINE_PON_COUNT!= \".\" && MIN(GERMLINE_PON_COUNT) > 5' -s GERMLINE_PON -m+ /data/output/tumor.strelka.post.processed.vcf.gz -O u | $TOOLS_DIR/bcftools/1.3.1/bcftools filter -e 'SOMATIC_PON_COUNT!=\".\" && MIN(SOMATIC_PON_COUNT) > 3' -s SOMATIC_PON -m+  -O z -o /data/output/tumor.pon.filtered.vcf.gz)",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.pon.filtered.vcf.gz -p vcf",
                "java -Xmx8G -cp $TOOLS_DIR/sage/1.1/sage.jar com.hartwig.hmftools.sage.SageHotspotAnnotation -source_vcf /data/output/tumor.pon.filtered.vcf.gz -hotspot_vcf /data/output/tumor.sage.pon.filter.vcf.gz -known_hotspots /data/resources/hotspots.tsv -out /data/output/tumor.sage.hotspots.annotated.vcf.gz",
                "$TOOLS_DIR/snpEff/4.3s/snpEff.sh $TOOLS_DIR/snpEff/4.3s/snpEff.jar /data/resources/snpeff.config GRCh37.75 /data/output/tumor.sage.hotspots.annotated.vcf.gz /data/output/tumor.snpeff.annotated.vcf",
                "$TOOLS_DIR/tabix/0.2.6/bgzip -f /data/output/tumor.snpeff.annotated.vcf",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.snpeff.annotated.vcf.gz -p vcf",
                "$TOOLS_DIR/bcftools/1.3.1/bcftools annotate -a /data/resources/dbsnp.vcf.gz -c ID -o /data/output/tumor.dbsnp.annotated.vcf.gz -O z /data/output/tumor.snpeff.annotated.vcf.gz",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.dbsnp.annotated.vcf.gz -p vcf",
                "$TOOLS_DIR/bcftools/1.3.1/bcftools annotate -a /data/resources/cosmic_collapsed.vcf.gz -c ID,INFO -o /data/output/tumor.cosmic.annotated.final.vcf.gz -O z /data/output/tumor.dbsnp.annotated.vcf.gz",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/tumor.cosmic.annotated.final.vcf.gz -p vcf");
    }

    @Override
    public void returnsExpectedOutput() {
        // ignored for now.
    }

    @Override
    protected void validateOutput(final SomaticCallerOutput output) {
        // ignored for now.
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runSomaticCaller(false).build();
    }


}