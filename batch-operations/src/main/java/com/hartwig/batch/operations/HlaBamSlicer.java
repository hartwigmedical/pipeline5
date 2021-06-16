package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.batch.operations.LilacBatch.LILAC_RESOURCES;
import static com.hartwig.batch.operations.rna.RnaCommon.RNA_COHORT_LOCATION_V37;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class HlaBamSlicer implements BatchOperation {

    private static final String HLA_BED_FILE = "hla_v37.bed";
    public static final String HLA_BAMS_BUCKET = "hla-bams";

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags)
    {
        // Inputs: SampleId,ExpectedAlleles
        final InputFileDescriptor runData = inputs.get();

        final String batchInputs = runData.inputValue();
        final String[] batchItems = batchInputs.split(",");
        final String sampleId = batchItems[0];

        // final String bamType = batchItems[1];

        final String sampleBam = String.format("%s.sorted.dups.bam", sampleId);

        commands.addCommand(() -> format("gsutil -u hmf-crunch cp %s/%s/%s* %s",
                RNA_COHORT_LOCATION_V37, sampleId, sampleBam, VmDirectories.INPUT));

        // get HLA bed for slicing
        commands.addCommand(() -> format("gsutil -u hmf-crunch cp gs://%s/%s %s",
                LILAC_RESOURCES, HLA_BED_FILE, VmDirectories.INPUT));


        // /opt/tools/sambamba/0.6.8/sambamba view -f bam ./samples/CPCT02020378T/CPCT02020378T.sorted.dups.bam -L /data/lilac/ref/hla.bed > ./samples/CPCT02020378T/CPCT02020378T.rna.hla.bam
            // download pilot Lilac jar
        final String sambamba = "sambamba/0.6.8/sambamba";

        final String slicedBam = String.format("%s.hla.bam", sampleId);

        commands.addCommand(() -> format("%s/%s slice %s/%s -L %s/%s -o %s/%s",
                VmDirectories.TOOLS, sambamba, VmDirectories.INPUT, sampleBam, VmDirectories.INPUT, HLA_BED_FILE,
                VmDirectories.OUTPUT, slicedBam));

        // commands.addCommand(() -> format("ls -l %s", VmDirectories.OUTPUT));

        final String slicedSortedBam = String.format("%s.rna.hla.bam", sampleId);

        // samtools sort -@ 8 -m 2G -T tmp -O bam Aligned.out.bam -o Aligned.sorted.bam
        final String[] sortArgs = {
                "sort", "-@", "8", "-m", "2G", "-T", "tmp",
                "-O", "bam", String.format("%s/%s", VmDirectories.OUTPUT, slicedBam),
                "-o", String.format("%s/%s", VmDirectories.OUTPUT, slicedSortedBam)};

        commands.addCommand(new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS, sortArgs));

        // create an index
        commands.addCommand(() -> format("%s/%s index %s/%s", VmDirectories.TOOLS, sambamba, VmDirectories.OUTPUT, slicedSortedBam));

        // copy the sliced RNA bam back to the HLA BAM directory
        final String sampleHlaDir = String.format("gs://%s/%s", HLA_BAMS_BUCKET, sampleId);
        commands.addCommand(() -> format("gsutil -m cp %s/%s* %s", VmDirectories.OUTPUT, slicedSortedBam, sampleHlaDir));

        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "lilac"), executionFlags));

        return ImmutableVirtualMachineJobDefinition.builder()
                .name("lilac")
                .startupCommand(commands)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .build();
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("HlaBamSlicer", "Slice RNA BAM for HLA analysis", OperationDescriptor.InputType.FLAT);
    }

}
