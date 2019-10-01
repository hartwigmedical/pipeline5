package com.hartwig.pipeline.tertiary.amber;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class AmberTest extends TertiaryStageTest<AmberOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockResource.addToStorage(storage, ResourceNames.REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, ResourceNames.AMBER_PON, "GermlineHetPon.hg19.bed", "GermlineSnp.hg19.bed");
    }

    @Override
    protected Stage<AmberOutput, SomaticRunMetadata> createVictim() {
        return new Amber(TestInputs.defaultPair());
    }

    @Override
    protected List<String> expectedResources() {
        return ImmutableList.of(resource(ResourceNames.REFERENCE_GENOME), resource(ResourceNames.AMBER_PON));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx32G -cp $TOOLS_DIR/amber/2.5/amber.jar com.hartwig.hmftools.amber.AmberApplication "
                + "-reference reference -reference_bam /data/input/reference.bam -tumor tumor -tumor_bam /data/input/tumor.bam -output_dir "
                + "/data/output -threads 16 -ref_genome /data/resources/reference.fasta -bed /data/resources/GermlineHetPon.hg19.bed "
                + "-snp_bed /data/resources/GermlineSnp.hg19.bed -threads $(grep -c '^processor' /proc/cpuinfo)");
    }

    @Override
    protected void validateOutput(final AmberOutput output) {
        assertThat(output.outputDirectory().bucket()).isEqualTo("run-reference-tumor-test/amber");
        assertThat(output.outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputDirectory().isDirectory()).isTrue();
    }
}