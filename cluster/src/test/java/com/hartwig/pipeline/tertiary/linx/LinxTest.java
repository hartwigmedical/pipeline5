package com.hartwig.pipeline.tertiary.linx;

import static com.hartwig.pipeline.resource.ResourceNames.ENSEMBL;
import static com.hartwig.pipeline.resource.ResourceNames.KNOWLEDGEBASES;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SV;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class LinxTest extends TertiaryStageTest<LinxOutput> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockResource.addToStorage(storage, REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, SV, "viral_host_ref.csv", "fragile_sites_hmf.csv", "line_elements.csv", "heli_rep_origins.bed");
        MockResource.addToStorage(storage, KNOWLEDGEBASES, "knownFusionPairs.csv", "knownPromiscuousFive.csv", "knownPromiscuousThree.csv");
        MockResource.addToStorage(storage, ENSEMBL, "ensembl_data_cache");
    }

    @Override
    protected List<String> expectedInputs() {
        return Collections.singletonList(input("run-reference-tumor-test/purple/results/", "results"));
    }

    @Override
    protected Stage<LinxOutput, SomaticRunMetadata> createVictim() {
        return new Linx(TestInputs.purpleOutput());
    }

    @Override
    protected List<String> expectedResources() {
        return ImmutableList.of(resource(REFERENCE_GENOME), resource(SV), resource(KNOWLEDGEBASES), resource(ENSEMBL));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx8G -jar $TOOLS_DIR/linx/1.4/linx.jar -sample tumor -sv_vcf "
                + "/data/input/tumor.purple.sv.vcf.gz -purple_dir /data/input/results -ref_genome /data/resources/reference.fasta "
                + "-output_dir /data/output -fragile_site_file /data/resources/fragile_sites_hmf.csv -line_element_file "
                + "/data/resources/line_elements.csv -replication_origins_file /data/resources/heli_rep_origins.bed -viral_hosts_file "
                + "/data/resources/viral_host_ref.csv -gene_transcripts_dir /data/resources -check_fusions -fusion_pairs_csv "
                + "/data/resources/knownFusionPairs.csv -promiscuous_five_csv /data/resources/knownPromiscuousFive.csv "
                + "-promiscuous_three_csv /data/resources/knownPromiscuousThree.csv -chaining_sv_limit 0 -check_drivers -write_vis_data");
    }

    @Test
    public void doesntRunWhenShallowEnabled() {
        assertThat(victim.shouldRun(Arguments.testDefaultsBuilder().shallow(true).runTertiary(true).build())).isFalse();
    }

    @Override
    protected void validateOutput(final LinxOutput output) {
        // no additional validation
    }
}