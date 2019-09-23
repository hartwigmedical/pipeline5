package com.hartwig.pipeline.tertiary.purple;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class PurpleTest extends TertiaryStageTest<PurpleOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockResource.addToStorage(storage, ResourceNames.REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, ResourceNames.GC_PROFILE, "gc_profile.cnp");
    }

    @Override
    protected Stage<PurpleOutput, SomaticRunMetadata> createVictim() {
        return new Purple(TestInputs.somaticCallerOutput(),
                TestInputs.structuralCallerOutput(),
                TestInputs.amberOutput(),
                TestInputs.cobaltOutput(),
                false);
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/somatic_caller/results/tumor.vcf.gz", "tumor.vcf.gz"),
                input(expectedRuntimeBucketName() + "/structural_caller/results/tumor.gridss.filtered.vcf.gz",
                        "tumor.gridss.filtered.vcf.gz"),
                input(expectedRuntimeBucketName() + "/structural_caller/results/tumor.gridss.filtered.vcf.gz.tbi",
                        "tumor.gridss.filtered.vcf.gz.tbi"),
                input(expectedRuntimeBucketName() + "/structural_caller/results/tumor.gridss.full.vcf.gz", "tumor.gridss.full.vcf.gz"),
                input(expectedRuntimeBucketName() + "/structural_caller/results/tumor.gridss.full.vcf.gz.tbi",
                        "tumor.gridss.full.vcf.gz.tbi"),
                input(expectedRuntimeBucketName() + "/amber/results/", "results"),
                input(expectedRuntimeBucketName() + "/cobalt/results/", "results"));
    }

    @Override
    protected List<String> expectedResources() {
        return ImmutableList.of(resource(ResourceNames.GC_PROFILE), resource(ResourceNames.REFERENCE_GENOME));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx8G -jar /opt/tools/purple/2.34/purple.jar -reference reference -tumor tumor -output_dir "
                + "/data/output -amber /data/input/results -cobalt /data/input/results -gc_profile /data/resources/gc_profile.cnp "
                + "-somatic_vcf /data/input/tumor.vcf.gz -structural_vcf /data/input/tumor.gridss.filtered.vcf.gz -sv_recovery_vcf "
                + "/data/input/tumor.gridss.full.vcf.gz -circos /opt/tools/circos/0.69.6/bin/circos -ref_genome "
                + "/data/resources/reference.fasta -threads $(grep -c '^processor' /proc/cpuinfo)");
    }

    @Test
    public void shallowModeUsesLowDepthSettings() {
        Purple victim = new Purple(TestInputs.somaticCallerOutput(),
                TestInputs.structuralCallerOutput(),
                TestInputs.amberOutput(),
                TestInputs.cobaltOutput(),
                true);
        assertThat(victim.commands(input(),
                StageRunner.resourceMap(victim.resources(storage, defaultArguments().resourceBucket(), runtimeBucket)))
                .get(0)
                .asBash()).contains("-highly_diploid_percentage 0.88 -somatic_min_total 100 -somatic_min_purity_spread 0.1");
    }

    @Override
    protected void validateOutput(final PurpleOutput output) {
        assertThat(output.outputDirectory().bucket()).isEqualTo(expectedRuntimeBucketName() + "/" + Purple.NAMESPACE);
        assertThat(output.outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputDirectory().isDirectory()).isTrue();
    }
}