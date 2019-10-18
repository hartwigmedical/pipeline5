package com.hartwig.pipeline.tertiary.purple;

import static org.assertj.core.api.Assertions.assertThat;

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
        MockResource.addToStorage(storage, ResourceNames.GRIDSS_REPEAT_MASKER_DB, "gridss.hg19.fa.out");
        MockResource.addToStorage(storage, ResourceNames.VIRUS_REFERENCE_GENOME, "human_virus.fa");
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
        return ImmutableList.of(resource(ResourceNames.GC_PROFILE),
                resource(ResourceNames.REFERENCE_GENOME),
                resource(ResourceNames.VIRUS_REFERENCE_GENOME),
                resource(ResourceNames.GRIDSS_REPEAT_MASKER_DB));
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of("java -Xmx8G -jar /opt/tools/purple/2.34/purple.jar -reference reference -tumor tumor -output_dir /data/output -amber /data/input/results -cobalt /data/input/results -gc_profile /data/resources/gc_profile.cnp -somatic_vcf /data/input/tumor.vcf.gz -structural_vcf /data/input/tumor.gridss.filtered.vcf.gz -sv_recovery_vcf /data/input/tumor.gridss.full.vcf.gz -circos /opt/tools/circos/0.69.6/bin/circos -ref_genome /data/resources/reference.fasta -threads $(grep -c '^processor' /proc/cpuinfo)",
                "gunzip -kd /data/output/tumor.purple.sv.vcf.gz",
                "(grep -E '^#' /data/output/tumor.purple.sv.vcf > /data/output/tumor.purple.sv.withbealn.vcf || true)",
                "cp /data/output/tumor.purple.sv.withbealn.vcf /data/output/tumor.purple.sv.missingbealn.vcf",
                "( (grep BEALN /data/output/tumor.purple.sv.vcf || true) | (grep -vE '^#' >> /data/output/tumor.purple.sv.withbealn.vcf || true) )",
                "( (grep -v BEALN /data/output/tumor.purple.sv.vcf || true) | (grep -vE '^#' >> /data/output/tumor.purple.sv.missingbealn.vcf || true) )",
                "java -Xmx8G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.5.2/gridss.jar gridss.AnnotateUntemplatedSequence REFERENCE_SEQUENCE=/data/resources/human_virus.fa INPUT=/data/output/tumor.purple.sv.missingbealn.vcf OUTPUT=/data/output/tumor.purple.sv.withannotation.vcf",
                "java -Xmx2G -jar /opt/tools/picard/2.18.27/picard.jar SortVcf I=/data/output/tumor.purple.sv.withbealn.vcf I=/data/output/tumor.purple.sv.withannotation.vcf O=/data/output/tumor.purple.viral_annotation.vcf.gz",
                "/bin/bash -e /opt/tools/gridss/2.5.2/failsafe_repeatmasker_invoker.sh /data/output/tumor.purple.viral_annotation.vcf.gz /data/output/tumor.purple.sv.ann.vcf.gz /data/resources/gridss.hg19.fa.out /opt/tools/gridss/2.5.2");
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
    public void returnsExpectedOutput() {
        // do nothing
    }

    @Override
    protected void validateOutput(final PurpleOutput output) {
        String bucketName = expectedRuntimeBucketName() + "/" + Purple.NAMESPACE;
        assertThat(output.outputDirectory().bucket()).isEqualTo(bucketName);
        assertThat(output.outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputDirectory().isDirectory()).isTrue();
        assertThat(output.somaticVcf().bucket()).isEqualTo(bucketName);
        assertThat(output.somaticVcf().path()).isEqualTo("results/tumor.purple.somatic.vcf.gz");
        assertThat(output.somaticVcf().isDirectory()).isFalse();
        assertThat(output.structuralVcf().bucket()).isEqualTo(bucketName);
        assertThat(output.structuralVcf().path()).isEqualTo("results/tumor.purple.sv.vcf.gz");
        assertThat(output.structuralVcf().isDirectory()).isFalse();
    }
}