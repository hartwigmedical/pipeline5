package com.hartwig.pipeline.calling.sage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.NoopPersistedDataset;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class SageSomaticCallerTest extends TertiaryStageTest<SageOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void shallowModeUsesHotspotQualOverride() {
        SageCaller victim = new SageCaller(TestInputs.defaultPair(),
                new NoopPersistedDataset(),
                SageConfiguration.somatic(TestInputs.REF_GENOME_37_RESOURCE_FILES, true));
        assertThat(victim.tumorReferenceCommands(input()).get(0).asBash()).contains("-hotspot_min_tumor_qual 40");
    }

    @Override
    protected Stage<SageOutput, SomaticRunMetadata> createVictim() {
        return new SageCaller(TestInputs.defaultPair(),
                new NoopPersistedDataset(),
                SageConfiguration.somatic(TestInputs.REF_GENOME_37_RESOURCE_FILES, false));
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of(
                "java -Xmx60G -jar /opt/tools/sage/3.0/sage.jar "
                        + "-tumor tumor -tumor_bam /data/input/tumor.bam "
                        + "-reference reference -reference_bam /data/input/reference.bam "
                        + "-hotspots /opt/resources/sage/37/KnownHotspots.somatic.37.vcf.gz "
                        + "-panel_bed /opt/resources/sage/37/ActionableCodingPanel.somatic.37.bed.gz "
                        + "-coverage_bed /opt/resources/sage/37/ActionableCodingPanel.somatic.37.bed.gz "
                        + "-high_confidence_bed /opt/resources/giab_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-ref_genome_version V37 "
                        + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ "
                        + "-write_bqr_data -write_bqr_plot "
                        + "-out /data/output/tumor.sage.somatic.vcf.gz -threads $(grep -c '^processor' /proc/cpuinfo)",
                "(/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER=\"PASS\"' /data/output/tumor.sage.somatic.vcf.gz -O z -o /data/output/tumor.sage.somatic.filtered.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.somatic.filtered.vcf.gz -p vcf");
    }

    @Override
    public void returnsExpectedOutput() {
        // not supported currently
    }

    @Override
    protected void validateOutput(final SageOutput output) {
        // not supported currently
    }

    @Override
    public void addsLogs() {
        // not supported currently
    }

    @Override
    protected void validatePersistedOutput(final SageOutput output) {
        assertThat(output.variants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/sage_somatic/tumor.sage.somatic.filtered.vcf.gz"));
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // ignore for now
    }
}