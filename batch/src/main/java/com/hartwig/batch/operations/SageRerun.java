package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.io.File;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.calling.somatic.SageCommandBuilder;
import com.hartwig.pipeline.calling.somatic.SageV2Application;
import com.hartwig.pipeline.calling.somatic.SageV2PostProcess;
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
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class SageRerun implements BatchOperation {

    private static final boolean PANEL_ONLY = false;
    private static final boolean CONVERT_TO_BAM = !PANEL_ONLY;

    public static GoogleStorageLocation sageArchiveDirectory(final String set) {
        return GoogleStorageLocation.of("hmf-sage", set, true);
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

        final InputFileDescriptor remoteTumorIndex = remoteTumorFile.index(".crai");
        final InputFileDescriptor remoteReferenceIndex = remoteReferenceFile.index(".crai");

        final String localTumorFile = localFilename(remoteTumorFile);
        final String localReferenceFile = localFilename(remoteReferenceFile);

        final String localTumorBam = CONVERT_TO_BAM ? localTumorFile.replace("cram", "bam") : localTumorFile;
        final String localReferenceBam = CONVERT_TO_BAM ? localReferenceFile.replace("cram", "bam") : localReferenceFile;

        // Prepare SnpEff
        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.HG37);
        commands.addCommand(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, resourceFiles.snpEffDb()));

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
            final InputFileDescriptor remoteRnaBamIndex = remoteRnaBam.index(".bai");
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

        SageV2Application sageV2Application = new SageV2Application(sageCommandBuilder);
        SageV2PostProcess sageV2PostProcess = new SageV2PostProcess(tumorSampleName, resourceFiles);
        SubStageInputOutput sageOutput = sageV2Application.andThen(sageV2PostProcess).apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addCommands(sageOutput.bash());

        // 8. Archive targeted output
        final GoogleStorageLocation archiveStorageLocation = sageArchiveDirectory(set);
        final OutputFile filteredOutputFile = sageOutput.outputFile();
        final OutputFile filteredOutputFileIndex = filteredOutputFile.index(".tbi");
        final OutputFile unfilteredOutputFile = sageV2Application.apply(SubStageInputOutput.empty(tumorSampleName)).outputFile();
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

        return VirtualMachineJobDefinition.sageCalling(commands, ResultsDirectory.defaultDirectory());
    }

    private OutputFile bqrFile(String sample, String extension) {
        String filename = String.format("%s.sage.bqr.%s", sample, extension);
        return ImmutableOutputFile.of(filename);
    }

    private List<BashCommand> cramToBam(String cram) {

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
