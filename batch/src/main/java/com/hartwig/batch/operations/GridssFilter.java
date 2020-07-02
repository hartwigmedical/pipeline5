package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.util.Collections;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssPassAndPonFilter;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssSomaticFilter;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class GridssFilter implements BatchOperation {

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.HG37);
        final String sample = inputs.get("sample").inputValue();
        final InputFileDescriptor inputVcf = inputs.get("inputVcf");
        final InputFileDescriptor inputVcfIndex = inputVcf.index(".tbi");

        final SubStageInputOutput postProcessing = new GridssSomaticFilter(resourceFiles).andThen(new GridssPassAndPonFilter())
                .apply(SubStageInputOutput.of(sample, inputFile(inputVcf.localDestination()), Collections.emptyList()));

        // 0. Download latest jar file
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s",
                "gs://hmf-gridss/resources/gripss.jar",
                "/opt/tools/gripss/" + Versions.GRIPSS + "/gripss.jar"));

        // 1. Download input files
        startupScript.addCommand(inputVcf::copyToLocalDestinationCommand);
        startupScript.addCommand(inputVcfIndex::copyToLocalDestinationCommand);

        //2. Run GRIPSS
        startupScript.addCommands(postProcessing.bash());

        // 3. Upload output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "gripss"), executionFlags));
        return VirtualMachineJobDefinition.structuralPostProcessCalling(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("GridssFilter", "Run GRIPSS", OperationDescriptor.InputType.JSON);
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

}
