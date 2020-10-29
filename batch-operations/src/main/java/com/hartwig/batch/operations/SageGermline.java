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
import com.hartwig.pipeline.calling.somatic.SageApplication;
import com.hartwig.pipeline.calling.somatic.SageCommandBuilder;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class SageGermline implements BatchOperation {

    private static String localFilename(InputFileDescriptor remote) {
        return format("%s/%s", VmDirectories.INPUT, new File(remote.inputValue()).getName());
    }

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.HG19);

        final InputFileDescriptor remoteReferenceFile = inputs.get("reference");
        final InputFileDescriptor remoteReferenceIndex = remoteReferenceFile.index();
        final String localReferenceFile = localFilename(remoteReferenceFile);
        final String localReferenceBam = localReferenceFile.replace("cram", "bam");
        final String referenceSampleName = inputs.get("referenceSample").inputValue();

        // Download latest jar file
        //        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s",
        //                "gs://batch-sage-validation/resources/sage.jar",
        //                "/opt/tools/sage/" + Versions.SAGE + "/sage.jar"));

        // Download normal
        startupScript.addCommand(() -> remoteReferenceFile.toCommandForm(localReferenceFile));
        startupScript.addCommand(() -> remoteReferenceIndex.toCommandForm(localFilename(remoteReferenceIndex)));

        final SageCommandBuilder sageCommandBuilder =
                new SageCommandBuilder(resourceFiles).germlineMode(referenceSampleName, localReferenceBam);
        final SageApplication sageApplication = new SageApplication(sageCommandBuilder);

        // Convert to bam if necessary
        if (!localReferenceFile.equals(localReferenceBam)) {
            startupScript.addCommands(cramToBam(localReferenceFile));
        }

        // Run post processing (NONE for germline)
        final SubStageInputOutput postProcessing = sageApplication.apply(SubStageInputOutput.empty(referenceSampleName));
        startupScript.addCommands(postProcessing.bash());

        // Store output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));

        return VirtualMachineJobDefinition.sageCalling(startupScript, ResultsDirectory.defaultDirectory());
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
        return OperationDescriptor.of("SageGermline", "Generate germline output", OperationDescriptor.InputType.JSON);
    }
}
