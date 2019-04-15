package com.hartwig.pipeline.calling.somatic;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.GoogleStorageInputOutput;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SomaticCaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(SomaticCaller.class);

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final Resource referenceGenome;

    SomaticCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final Resources resources) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.referenceGenome = resources.referenceGenome();
    }

    public SomaticCallerOutput run(AlignmentPair pair) {
        RuntimeBucket runtimeBucket =
                RuntimeBucket.from(storage, pair.reference().sample().name(), pair.tumor().sample().name(), arguments);

        referenceGenome.copyInto(runtimeBucket);
        BashStartupScript strelkaBash = BashStartupScript.of("/data/output", "/data/logs/strelka.log")
                .addLine(inputData(pair.reference().finalBamLocation()))
                .addLine(inputData(pair.tumor().finalBamLocation()))
                .addCommand(new ResourceDownload(referenceGenome))
                .addLine("/data/tools/strelka_v1.0.14/bin/configureStrelkaWorkflow.pl");

        LOGGER.info(strelkaBash.asUnixString());

        computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.somaticCalling(strelkaBash));
        return null;
    }

    private String inputData(final GoogleStorageLocation location) {
        return new GoogleStorageInputOutput(location.bucket()).copyToLocal(location.path(), "/data/input/");
    }
}
