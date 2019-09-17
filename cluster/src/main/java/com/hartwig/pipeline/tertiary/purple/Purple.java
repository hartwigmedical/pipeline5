package com.hartwig.pipeline.tertiary.purple;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tools.Versions;
import com.hartwig.pipeline.trace.StageTrace;

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

    public PurpleOutput run(SomaticRunMetadata metadata, AlignmentPair pair, SomaticCallerOutput somaticCallerOutput,
            StructuralCallerOutput structuralCallerOutput, CobaltOutput cobaltOutput, AmberOutput amberOutput) {

        if (!arguments.runTertiary()) {
            return PurpleOutput.builder().status(PipelineStatus.SKIPPED).build();
        }

        StageTrace trace = new StageTrace(NAMESPACE, metadata.runName(), StageTrace.ExecutorType.COMPUTE_ENGINE).start();

        String tumorSampleName = pair.tumor().sample();
        String referenceSampleName = pair.reference().sample();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, metadata, arguments);
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        ResourceDownload gcProfileDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.GC_PROFILE, runtimeBucket);
        ResourceDownload referenceGenomeDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.REFERENCE_GENOME, runtimeBucket);
        bash.addCommand(gcProfileDownload).addCommand(referenceGenomeDownload);

        InputDownload somaticVcfDownload = new InputDownload(somaticCallerOutput.finalSomaticVcf());
        InputDownload structuralVcfDownload = new InputDownload(structuralCallerOutput.filteredVcf());
        InputDownload structuralVcfIndexDownload = new InputDownload(structuralCallerOutput.filteredVcfIndex());
        InputDownload svRecoveryVcfDownload = new InputDownload(structuralCallerOutput.fullVcf());
        InputDownload svRecoveryVcfIndexDownload = new InputDownload(structuralCallerOutput.fullVcfIndex());
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
                VmDirectories.TOOLS + "/circos/" + Versions.CIRCOS + "/bin/circos",
                referenceGenomeDownload.find("fasta", "fa"),
                arguments.shallow()));
        bash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        PipelineStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.purple(bash, resultsDirectory));
        trace.stop();
        return PurpleOutput.builder()
                .status(status)
                .maybeOutputDirectory(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path(), true))
                .addReportComponents(new EntireOutputComponent(runtimeBucket, Folder.from(), NAMESPACE, resultsDirectory))
                .build();
    }
}
