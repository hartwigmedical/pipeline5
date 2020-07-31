package com.hartwig.pipeline.tertiary.linx;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class LinxTest extends TertiaryStageTest<LinxOutput> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected List<String> expectedInputs() {
        return Collections.singletonList(input("run-reference-tumor-test/purple/results/", "results"));
    }

    @Override
    protected Stage<LinxOutput, SomaticRunMetadata> createVictim() {
        return new Linx(TestInputs.purpleOutput(), TestInputs.HG37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx8G -jar /opt/tools/linx/1.7/linx.jar -sample tumor -sv_vcf "
                + "/data/input/tumor.purple.sv.vcf.gz -purple_dir /data/input/results "
                + "-ref_genome /opt/resources/reference_genome/hg37/Homo_sapiens.GRCh37.GATK.illumina.fasta -ref_genome_version 37 "
                + "-output_dir /data/output -fragile_site_file "
                + "/opt/resources/sv/hg37/fragile_sites_hmf.csv -line_element_file /opt/resources/sv/hg37/line_elements.csv "
                + "-replication_origins_file /opt/resources/sv/hg37/heli_rep_origins.bed -viral_hosts_file /opt/resources/sv/viral_host_ref.csv "
                + "-gene_transcripts_dir /opt/resources/ensembl/hg37/ensembl_data_cache -check_fusions -fusion_pairs_csv "
                + "/opt/resources/knowledgebases/output/knownFusionPairs.csv -promiscuous_five_csv "
                + "/opt/resources/knowledgebases/output/knownPromiscuousFive.csv -promiscuous_three_csv "
                + "/opt/resources/knowledgebases/output/knownPromiscuousThree.csv -chaining_sv_limit 0 -check_drivers -write_vis_data");
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final LinxOutput output) {
        // no additional validation
    }
}