package com.hartwig.pipeline.cram;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.input.SingleSampleRunMetadata.SampleType;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class CramConversionTest extends StageTest<CramOutput, SingleSampleRunMetadata> {
    private static final String BUCKET_NAME = TestInputs.REFERENCE_BUCKET;

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().outputCram(false).build();
    }

    @Override
    public void disabledAppropriately() {
        assertThat(victim.shouldRun(createDisabledArguments())).isFalse();
    }

    @Override
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(Arguments.testDefaultsBuilder().outputCram(true).build())).isTrue();
    }

    @Override
    protected Stage<CramOutput, SingleSampleRunMetadata> createVictim() {
        return new CramConversion(TestInputs.referenceAlignmentOutput(), SampleType.REFERENCE, TestInputs.REF_GENOME_38_RESOURCE_FILES);
    }

    @Override
    protected SingleSampleRunMetadata input() {
        return TestInputs.referenceRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(BUCKET_NAME + "/aligner/results/reference.bam", "reference.bam"));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return BUCKET_NAME;
    }

    @Override
    protected List<String> expectedCommands() {
        String samtools = "/opt/tools/samtools/1.20/samtools";
        String input = "/data/input/reference.bam";
        String output = "/data/output/reference.cram";
        return ImmutableList.of(format("%s view -T %s -o %s -O cram,embed_ref=1 -@ $(grep -c '^processor' /proc/cpuinfo) %s",
                samtools,
                TestInputs.REF_GENOME_38_RESOURCE_FILES.refGenomeFile(),
                output,
                input),
                format("%s reheader --no-PG --in-place --command 'grep -v ^@PG' %s", samtools, output),
                format("%s index %s", samtools, output),
                format("java -Xmx4G -cp /opt/tools/bamcomp/1.3/bamcomp.jar com.hartwig.bamcomp.BamCompMain -r /opt/resources/reference_genome/38/Homo_sapiens_assembly38.alt.masked.fasta -1 %s -2 %s -n 16 --samtools-binary /opt/tools/samtools/1.20/samtools --sambamba-binary /opt/tools/sambamba/0.6.8/sambamba",
                        input,
                        output));
    }

    @Override
    protected void validateOutput(final CramOutput output) {
        // no additional validation
    }

    @Override
    protected void validatePersistedOutput(final CramOutput output) {
        assertThat(output).isEqualTo(CramOutput.builder().status(PipelineStatus.PERSISTED).build());
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.ALIGNED_READS,
                        TestInputs.referenceRunMetadata().barcode(),
                        new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), CramConversion.NAMESPACE, "reference.cram")),
                new AddDatatype(DataType.ALIGNED_READS_INDEX,
                        TestInputs.referenceRunMetadata().barcode(),
                        new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), CramConversion.NAMESPACE, "reference.cram.crai")));
    }
}
