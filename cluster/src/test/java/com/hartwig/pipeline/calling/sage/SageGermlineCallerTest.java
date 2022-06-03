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

public class SageGermlineCallerTest extends TertiaryStageTest<SageOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<SageOutput, SomaticRunMetadata> createVictim() {
        return new SageGermlineCaller(TestInputs.defaultPair(),
                new NoopPersistedDataset(),
                TestInputs.REF_GENOME_37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of(
                "java -Xmx15G -jar /opt/tools/sage/pilot/sage.jar "
                        + "-tumor reference -tumor_bam /data/input/reference.bam "
                        + "-reference tumor -reference_bam /data/input/tumor.bam "
                        + "-hotspots /opt/resources/sage/37/KnownHotspots.germline.37.vcf.gz "
                        + "-panel_bed /opt/resources/sage/37/ActionableCodingPanel.germline.37.bed.gz "
                        + "-hotspot_min_tumor_qual 50 -panel_min_tumor_qual 75 -hotspot_max_germline_vaf 100 "
                        + "-hotspot_max_germline_rel_raw_base_qual 100 -panel_max_germline_vaf 100 -panel_max_germline_rel_raw_base_qual 100 "
                        + "-mnv_filter_enabled false "
                        + "-panel_only -coverage_bed /opt/resources/sage/37/CoverageCodingPanel.germline.37.bed.gz "
                        + "-high_confidence_bed /opt/resources/giab_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-ref_genome_version V37 "
                        + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ "
                        + "-write_bqr_data -write_bqr_plot "
                        + "-out /data/output/tumor.sage.germline.vcf.gz "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo)",
                "(/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER=\"PASS\"' /data/output/tumor.sage.germline.vcf.gz -O z -o /data/output/tumor.sage.germline.filtered.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.germline.filtered.vcf.gz -p vcf");
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
                "set/sage_germline/tumor.sage.germline.filtered.vcf.gz"));
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // ignore for now
    }
}