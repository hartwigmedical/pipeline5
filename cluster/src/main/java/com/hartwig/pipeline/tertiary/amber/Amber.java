package com.hartwig.pipeline.tertiary.amber;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.results.EntireOutputComponent;
import com.hartwig.pipeline.trace.StageTrace;

public class Amber {

    public static final String NAMESPACE = "amber";
    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    Amber(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public AmberOutput run(SomaticRunMetadata metadata, AlignmentPair pair) {

        if (!arguments.runTertiary()) {
            return AmberOutput.builder().status(PipelineStatus.SKIPPED).build();
        }

        StageTrace trace = new StageTrace(NAMESPACE, StageTrace.ExecutorType.COMPUTE_ENGINE).start();

        String tumorSampleName = pair.tumor().sample();
        String referenceSampleName = pair.reference().sample();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, metadata, arguments);
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        ResourceDownload referenceGenomeDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.REFERENCE_GENOME, runtimeBucket);
        ResourceDownload amberResourceDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.AMBER_PON, runtimeBucket);
        bash.addCommand(referenceGenomeDownload).addCommand(amberResourceDownload);

        InputDownload tumorBam = new InputDownload(pair.tumor().finalBamLocation());
        InputDownload tumorBai = new InputDownload(pair.tumor().finalBaiLocation());
        InputDownload referenceBam = new InputDownload(pair.reference().finalBamLocation());
        InputDownload referenceBai = new InputDownload(pair.reference().finalBaiLocation());
        bash.addCommand(tumorBam).addCommand(referenceBam).addCommand(tumorBai).addCommand(referenceBai);

        bash.addCommand(new AmberApplicationCommand(referenceSampleName,
                referenceBam.getLocalTargetPath(),
                tumorSampleName,
                tumorBam.getLocalTargetPath(),
                referenceGenomeDownload.find("fasta", "fa"),
                amberResourceDownload.find("GermlineHetPon.hg19.bed"),
                amberResourceDownload.find("GermlineSnp.hg19.bed")));
        bash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        PipelineStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.amber(bash, resultsDirectory));
        trace.stop();
        return AmberOutput.builder()
                .status(status)
                .maybeOutputDirectory(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path(), true))
                .addReportComponents(new EntireOutputComponent(runtimeBucket, pair, NAMESPACE, resultsDirectory))
                .build();
    }
}
