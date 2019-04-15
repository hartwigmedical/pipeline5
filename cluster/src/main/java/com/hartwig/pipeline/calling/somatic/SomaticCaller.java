package com.hartwig.pipeline.calling.somatic;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.RuntimeBucket;

public class SomaticCaller {

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final GoogleCredentials credentials;
    private final String referenceGenomeBucket;

    SomaticCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final GoogleCredentials credentials,
            final String referenceGenomeBucket) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.credentials = credentials;
        this.referenceGenomeBucket = referenceGenomeBucket;
    }

    public SomaticCallerOutput run(AlignmentPair pair) {
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, arguments.sampleId(), arguments);
        computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.somaticCalling());
        return null;
    }
}
