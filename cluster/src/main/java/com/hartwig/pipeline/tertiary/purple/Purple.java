package com.hartwig.pipeline.tertiary.purple;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;

public class Purple {

    private static final String NAMESPACE = "purple";
    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    Purple(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public PurpleOutput run(AlignmentPair pair, GoogleStorageLocation somaticVcf, GoogleStorageLocation structuralVcf,
            GoogleStorageLocation svRecoveryVcf, GoogleStorageLocation cobaltOutput, GoogleStorageLocation amberOutput) {
        String tumorSampleName = pair.tumor().sample().name();
        String referenceSampleName = pair.reference().sample().name();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, referenceSampleName, tumorSampleName, arguments);
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        ResourceDownload gcProfileDownload = ResourceDownload.from(storage, "cobalt-gc", runtimeBucket);
        ResourceDownload referenceGenomeDownload = ResourceDownload.from(storage, "reference_genome", runtimeBucket);
        bash.addCommand(gcProfileDownload).addCommand(referenceGenomeDownload);

        InputDownload somaticVcfDownload = new InputDownload(somaticVcf);
        InputDownload structuralVcfDownload = new InputDownload(structuralVcf);
        InputDownload svRecoveryVcfDownload = new InputDownload(svRecoveryVcf);
        InputDownload amberOutputDownload = new InputDownload(amberOutput);
        InputDownload cobaltOutputDownload = new InputDownload(cobaltOutput);
        bash.addCommand(somaticVcfDownload)
                .addCommand(structuralVcfDownload)
                .addCommand(svRecoveryVcfDownload)
                .addCommand(amberOutputDownload)
                .addCommand(cobaltOutputDownload);

        bash.addCommand(new PurpleApplicationCommand(referenceSampleName,
                tumorSampleName,
                amberOutputDownload.getLocalTargetPath(),
                cobaltOutputDownload.getLocalTargetPath(),
                gcProfileDownload.find("cnp"),
                somaticVcfDownload.getLocalTargetPath(),
                structuralVcfDownload.getLocalTargetPath(),
                svRecoveryVcfDownload.getLocalTargetPath(),
                VmDirectories.TOOLS + "/"));
        bash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        JobStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.purple(bash, resultsDirectory));
        return PurpleOutput.builder().status(status).build();
    }
}
