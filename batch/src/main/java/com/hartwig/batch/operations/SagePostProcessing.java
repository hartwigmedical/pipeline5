package com.hartwig.batch.operations;

import java.io.File;
import java.util.Collections;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.ImmutableInputFileDescriptor;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.somatic.SagePostProcess;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
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

public class SagePostProcessing implements BatchOperation {

    private static final RefGenomeVersion REF_GENOME_VERSION = RefGenomeVersion.HG38;

    public static GoogleStorageLocation sageArchiveDirectory(final String set) {
        return GoogleStorageLocation.of("hmf-sage-hg38", set, true);
    }

    public static GoogleStorageLocation hg38RemoteUnfilteredVcfArchivePath(final String set, final String sample) {
        return GoogleStorageLocation.of("hg38-pon-pipeline-results",
                set + "/sage/" + sample + ".sage.somatic.vcf.gz",
                false);
    }

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(REF_GENOME_VERSION);
        final InputFileDescriptor setDescriptor = inputs.get("set");
        final String set = setDescriptor.inputValue();
        final String sample = inputs.get("tumorSample").inputValue();
        final InputFileDescriptor inputVcf = inputFile(setDescriptor, hg38RemoteUnfilteredVcfArchivePath(set, sample));
        final InputFileDescriptor inputVcfIndex = inputVcf.index();

        // Prepare SnpEff
        startupScript.addCommand(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, resourceFiles.snpEffDb()));

        // 1. Download input files
        startupScript.addCommand(inputVcf::copyToLocalDestinationCommand);
        startupScript.addCommand(inputVcfIndex::copyToLocalDestinationCommand);

        //2. Run GRIPSS
        SagePostProcess sagePostProcess = new SagePostProcess(sample, resourceFiles);
        SubStageInputOutput sageOutput = sagePostProcess.apply(SubStageInputOutput.of(sample, inputFile(inputVcf.localDestination()), Collections.emptyList()));
        startupScript.addCommands(sageOutput.bash());

        //3. Upload targeted output
        final GoogleStorageLocation archiveStorageLocation = sageArchiveDirectory(set);
        final OutputFile filteredOutputFile = sageOutput.outputFile();
        final OutputFile filteredOutputFileIndex = filteredOutputFile.index(".tbi");
        startupScript.addCommand(() -> filteredOutputFile.copyToRemoteLocation(archiveStorageLocation));
        startupScript.addCommand(() -> filteredOutputFileIndex.copyToRemoteLocation(archiveStorageLocation));

        // 4. Upload output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));
        return VirtualMachineJobDefinition.sageCalling(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SagePostProcessing", "Run SAGE Post Processing", OperationDescriptor.InputType.JSON);
    }

    private OutputFile inputFile(final String file) {
        return new OutputFile() {
            @Override
            public String fileName() {
                return file;
            }

            @Override
            public String path() {
                return file;
            }
        };
    }

    private static InputFileDescriptor inputFile(final InputFileDescriptor inputTemplate, final GoogleStorageLocation location) {
        if (location.isDirectory()) {
            throw new IllegalArgumentException();
        }
        final String remoteFilename = location.bucket() + File.separator + location.path();
        return ImmutableInputFileDescriptor.builder().from(inputTemplate).inputValue(remoteFilename).build();
    }

}
