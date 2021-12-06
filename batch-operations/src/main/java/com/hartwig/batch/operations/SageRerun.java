package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.io.File;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.calling.sage.SageApplication;
import com.hartwig.pipeline.calling.sage.SageCommandBuilder;
import com.hartwig.pipeline.calling.sage.SageSomaticPostProcess;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableOutputFile;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.UnzipToDirectoryCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class SageRerun implements BatchOperation {

    private static final boolean PANEL_ONLY = false;
    private static final boolean CONVERT_TO_BAM = !PANEL_ONLY;

    public static GoogleStorageLocation sageArchiveDirectory(final String set) {
        return GoogleStorageLocation.of("hmf-sage", set, true);
    }

    public static GoogleStorageLocation sageSomaticFilteredFile(final String set, final String sample) {
        return GoogleStorageLocation.of("hmf-sage", set + "/" + sample + ".sage.somatic.filtered.vcf.gz", false);
    }

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript commands, final RuntimeFiles executionFlags) {

        // Inputs
        final String set = inputs.get("set").inputValue();
        final String tumorSampleName = inputs.get("tumor_sample").inputValue();
        final String referenceSampleName = inputs.get("ref_sample").inputValue();
        final InputFileDescriptor remoteTumorFile = inputs.get("tumor_cram");
        final InputFileDescriptor remoteReferenceFile = inputs.get("ref_cram");

        final InputFileDescriptor remoteTumorIndex = remoteTumorFile.index();
        final InputFileDescriptor remoteReferenceIndex = remoteReferenceFile.index();

        final String localTumorFile = localFilename(remoteTumorFile);
        final String localReferenceFile = localFilename(remoteReferenceFile);

        final String localTumorBam = CONVERT_TO_BAM ? localTumorFile.replace("cram", "bam") : localTumorFile;
        final String localReferenceBam = CONVERT_TO_BAM ? localReferenceFile.replace("cram", "bam") : localReferenceFile;

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.V37);

        // Download tumor
        commands.addCommand(() -> remoteTumorFile.toCommandForm(localTumorFile));
        commands.addCommand(() -> remoteTumorIndex.toCommandForm(localFilename(remoteTumorIndex)));

        // Download normal
        commands.addCommand(() -> remoteReferenceFile.toCommandForm(localReferenceFile));
        commands.addCommand(() -> remoteReferenceIndex.toCommandForm(localFilename(remoteReferenceIndex)));

        final SageCommandBuilder sageCommandBuilder =
                new SageCommandBuilder(resourceFiles).addReference(referenceSampleName, localReferenceBam)
                        .addTumor(tumorSampleName, localTumorBam);

        if (PANEL_ONLY) {
            sageCommandBuilder.panelOnly();
        }

        if (inputs.contains("rna")) {
            final InputFileDescriptor remoteRnaBam = inputs.get("rna");
            final InputFileDescriptor remoteRnaBamIndex = remoteRnaBam.index();
            final String localRnaBam = localFilename(remoteRnaBam);

            // Download rna
            commands.addCommand(() -> remoteRnaBam.toCommandForm(localRnaBam));
            commands.addCommand(() -> remoteRnaBamIndex.toCommandForm(localFilename(remoteRnaBamIndex)));

            // Add to sage application
            sageCommandBuilder.addReference(referenceSampleName + "NA", localRnaBam);
        }

        // Convert to bam if necessary
        if (!localTumorFile.equals(localTumorBam)) {
            commands.addCommands(cramToBam(localTumorFile));
        }
        if (!localReferenceFile.equals(localReferenceBam)) {
            commands.addCommands(cramToBam(localReferenceFile));
        }

        SageApplication sageApplication = new SageApplication(sageCommandBuilder);
        SageSomaticPostProcess sagePostProcess = new SageSomaticPostProcess(tumorSampleName, resourceFiles);
        SubStageInputOutput sageOutput = sageApplication.andThen(sagePostProcess).apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addCommands(sageOutput.bash());

        // 8. Archive targeted output
        final GoogleStorageLocation archiveStorageLocation = sageArchiveDirectory(set);
        final OutputFile filteredOutputFile = sageOutput.outputFile();
        final OutputFile filteredOutputFileIndex = filteredOutputFile.index(".tbi");
        final OutputFile unfilteredOutputFile = sageApplication.apply(SubStageInputOutput.empty(tumorSampleName)).outputFile();
        final OutputFile unfilteredOutputFileIndex = unfilteredOutputFile.index(".tbi");

        commands.addCommand(() -> filteredOutputFile.copyToRemoteLocation(archiveStorageLocation));
        commands.addCommand(() -> filteredOutputFileIndex.copyToRemoteLocation(archiveStorageLocation));
        commands.addCommand(() -> unfilteredOutputFile.copyToRemoteLocation(archiveStorageLocation));
        commands.addCommand(() -> unfilteredOutputFileIndex.copyToRemoteLocation(archiveStorageLocation));
        commands.addCommand(() -> bqrFile(tumorSampleName, "png").copyToRemoteLocation(archiveStorageLocation));
        commands.addCommand(() -> bqrFile(tumorSampleName, "tsv").copyToRemoteLocation(archiveStorageLocation));
        commands.addCommand(() -> bqrFile(referenceSampleName, "png").copyToRemoteLocation(archiveStorageLocation));
        commands.addCommand(() -> bqrFile(referenceSampleName, "tsv").copyToRemoteLocation(archiveStorageLocation));

        // Store output
        commands.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));

        return VirtualMachineJobDefinition.sageSomaticCalling(commands, ResultsDirectory.defaultDirectory());
    }

    private OutputFile bqrFile(String sample, String extension) {
        String filename = String.format("%s.sage.bqr.%s", sample, extension);
        return ImmutableOutputFile.of(filename);
    }

    static List<BashCommand> cramToBam(String cram) {

        final String output = cram.replace("cram", "bam");
        final BashCommand toBam = new VersionedToolCommand("samtools",
                "samtools",
                Versions.SAMTOOLS,
                "view",
                "-o",
                output,
                "-O",
                "bam",
                "-@",
                Bash.allCpus(),
                cram);

        final BashCommand index =
                new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS, "index", "-@", Bash.allCpus(), output);

        return Lists.newArrayList(toBam, index);
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SageRerun", "Generate sage output", OperationDescriptor.InputType.JSON);
    }

    private static String localFilename(InputFileDescriptor remote) {
        return format("%s/%s", VmDirectories.INPUT, new File(remote.inputValue()).getName());
    }

    private BashCommand downloadExperimentalVersion() {
        return () -> format("gsutil -u hmf-crunch cp %s %s",
                "gs://batch-sage-validation/resources/sage.jar",
                "/opt/tools/sage/" + Versions.SAGE + "/sage.jar");
    }
}
