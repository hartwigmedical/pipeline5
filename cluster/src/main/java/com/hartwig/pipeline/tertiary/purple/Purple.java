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
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tools.Versions;

public class Purple {

    static final String NAMESPACE = "purple";
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

    public PurpleOutput run(AlignmentPair pair, SomaticCallerOutput somaticCallerOutput, StructuralCallerOutput structuralCallerOutput,
            CobaltOutput cobaltOutput, AmberOutput amberOutput) {

        if (!arguments.runTertiary()) {
            return PurpleOutput.builder().status(JobStatus.SKIPPED).build();
        }

        String tumorSampleName = pair.tumor().sample().name();
        String referenceSampleName = pair.reference().sample().name();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, referenceSampleName, tumorSampleName, arguments);
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        ResourceDownload gcProfileDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.GC_PROFILE, runtimeBucket);
        ResourceDownload referenceGenomeDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.REFERENCE_GENOME, runtimeBucket);
        bash.addCommand(gcProfileDownload).addCommand(referenceGenomeDownload);

        InputDownload somaticVcfDownload = new InputDownload(somaticCallerOutput.finalSomaticVcf());
        InputDownload structuralVcfDownload = new InputDownload(structuralCallerOutput.structuralVcf());
        InputDownload structuralVcfIndexDownload = new InputDownload(structuralCallerOutput.structuralVcfIndex());
        InputDownload svRecoveryVcfIndexDownload = new InputDownload(structuralCallerOutput.svRecoveryVcfIndex());
        InputDownload svRecoveryVcfDownload = new InputDownload(structuralCallerOutput.svRecoveryVcf());
        InputDownload amberOutputDownload = new InputDownload(amberOutput.outputDirectory());
        InputDownload cobaltOutputDownload = new InputDownload(cobaltOutput.outputDirectory());
        bash.addCommand(somaticVcfDownload)
                .addCommand(structuralVcfDownload)
                .addCommand(svRecoveryVcfDownload)
                .addCommand(amberOutputDownload)
                .addCommand(cobaltOutputDownload)
                .addCommand(structuralVcfIndexDownload)
                .addCommand(svRecoveryVcfIndexDownload);

        bash.addCommand(new PurpleApplicationCommand(referenceSampleName,
                tumorSampleName,
                amberOutputDownload.getLocalTargetPath(),
                cobaltOutputDownload.getLocalTargetPath(),
                gcProfileDownload.find("cnp"),
                somaticVcfDownload.getLocalTargetPath(),
                structuralVcfDownload.getLocalTargetPath(),
                svRecoveryVcfDownload.getLocalTargetPath(),
                VmDirectories.TOOLS + "/circos/" + Versions.CIRCOS + "/bin/circos"));
        bash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        JobStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.purple(bash, resultsDirectory));
        return PurpleOutput.builder()
                .status(status)
                .maybeOutputDirectory(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path(), true))
                .build();
    }
}
