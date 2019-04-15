package com.hartwig.pipeline.calling.somatic;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.GoogleStorageInputOutput;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.RuntimeBucket;

public class SomaticCaller {

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final String referenceGenomeBucket;

    SomaticCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final String referenceGenomeBucket) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.referenceGenomeBucket = referenceGenomeBucket;
    }

    public SomaticCallerOutput run(AlignmentPair pair) {
        RuntimeBucket runtimeBucket =
                RuntimeBucket.from(storage, pair.reference().sample().name(), pair.tumor().sample().name(), arguments);

        BashStartupScript strelkaBash = BashStartupScript.of("/tmp/strelka.log", "/data/output")
                .addLine(inputData(pair.reference().finalBamLocation()))
                .addLine(inputData(pair.tumor().finalBamLocation()))
                .addLine("/data/tools/strelka_v1.0.14/bin/configureStrelkaWorkflow.pl");
        computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.somaticCalling(strelkaBash));
        return null;
    }

    private String inputData(final GoogleStorageLocation location) {
        return new GoogleStorageInputOutput(location.bucket()).copyToLocal(location.path(), "/data/input/");
    }
}
