package com.hartwig.pipeline.tertiary.cobalt;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class Cobalt {

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    Cobalt(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public CobaltOutput run(AlignmentPair pair) {
        String tumorSampleName = pair.tumor().sample().name();
        String referenceSampleName = pair.reference().sample().name();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, referenceSampleName, tumorSampleName, arguments);
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        ResourceDownload cobaltResourceDownload = ResourceDownload.from(storage, "cobalt-gc", runtimeBucket);
        bash.addCommand(cobaltResourceDownload);

        InputDownload tumorBam = new InputDownload(pair.tumor().finalBamLocation());
        InputDownload tumorBai = new InputDownload(pair.tumor().finalBaiLocation());
        InputDownload referenceBam = new InputDownload(pair.reference().finalBamLocation());
        InputDownload referenceBai = new InputDownload(pair.reference().finalBaiLocation());
        bash.addCommand(tumorBam).addCommand(referenceBam).addCommand(tumorBai).addCommand(referenceBai);

        String cobaltOutput = OutputFile.of(tumorSampleName, "cobalt").path();
        bash.addCommand(new CobaltApplicationCommand(referenceSampleName,
                referenceBam.getLocalTargetPath(),
                tumorSampleName,
                tumorBam.getLocalTargetPath(),
                cobaltResourceDownload.find("cnp")));
        bash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        JobStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.cobalt(bash));
        return CobaltOutput.builder().status(status).cobaltFile(GoogleStorageLocation.of(runtimeBucket.name(), cobaltOutput)).build();
    }
}
