package com.hartwig.pipeline.tertiary.bachelor;

import static com.hartwig.pipeline.resource.ResourceNames.BACHELOR;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class BachelorTest extends TertiaryStageTest<BachelorOutput> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockResource.addToStorage(storage, REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, BACHELOR, "bachelor_clinvar_filters.csv", "bachelor_hmf.xml");
    }

    @Override
    protected Stage<BachelorOutput, SomaticRunMetadata> createVictim() {
        return new Bachelor(TestInputs.purpleOutput(), TestInputs.tumorAlignmentOutput(), TestInputs.germlineCallerOutput());
    }

    @Override
    protected List<String> expectedResources() {
        return ImmutableList.of(resource(REFERENCE_GENOME), resource(BACHELOR));
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/purple/results/", "results"),
                input("run-tumor-test/aligner/results/tumor.bam", "tumor.bam"),
                input("run-tumor-test/aligner/results/tumor.bam.bai", "tumor.bam.bai"),
                input("run-reference-test/germline_caller/reference.germline.vcf.gz", "reference.germline.vcf.gz"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx8G -jar /opt/tools/bachelor/1.9/bachelor.jar -sample tumor -germline_vcf "
                + "/data/input/reference.germline.vcf.gz -tumor_bam_file /data/input/tumor.bam -purple_data_dir "
                + "/data/input/results -xml_config /data/resources/bachelor_hmf.xml -ext_filter_file "
                + "/data/resources/bachelor_clinvar_filters.csv -ref_genome /data/resources/reference.fasta -output_dir /data/output "
                + "-log_debug");
    }

    @Override
    protected void validateOutput(final BachelorOutput output) {
        // no additional validation
    }
}