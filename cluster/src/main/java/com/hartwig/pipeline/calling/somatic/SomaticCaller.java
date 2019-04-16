package com.hartwig.pipeline.calling.somatic;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceLocation;
import com.hartwig.pipeline.resource.Resources;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SomaticCaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(SomaticCaller.class);

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final Resource referenceGenome;
    private final Resource strelkaConfig;

    SomaticCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final Resources resources) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.referenceGenome = resources.referenceGenome();
        this.strelkaConfig = resources.strelkaConfig();
    }

    public SomaticCallerOutput run(AlignmentPair pair) {
        LOGGER.info("Starting Somatic Calling");
        RuntimeBucket runtimeBucket =
                RuntimeBucket.from(storage, pair.reference().sample().name(), pair.tumor().sample().name(), arguments);
        ResourceLocation referenceGenomeLocation = referenceGenome.copyInto(runtimeBucket);
        ResourceLocation strelkaConfigLocation = strelkaConfig.copyInto(runtimeBucket);

        InputDownload downloadReferenceBam = new InputDownload(pair.reference().finalBamLocation());
        InputDownload downloadReferenceBai = new InputDownload(pair.reference().finalBaiLocation());
        InputDownload downloadTumorBam = new InputDownload(pair.tumor().finalBamLocation());
        InputDownload downloadTumorBai = new InputDownload(pair.tumor().finalBaiLocation());

        ResourceDownload referenceGenomeDownload = new ResourceDownload(referenceGenomeLocation, runtimeBucket);
        String referenceGenomeFile = localReferenceGenomeFile(referenceGenomeLocation, referenceGenomeDownload);

        ResourceDownload configDownload = new ResourceDownload(strelkaConfigLocation, runtimeBucket);
        String strelkaConfigFile = localStrelkaConfigFile(strelkaConfigLocation, configDownload);

        BashStartupScript strelkaBash = BashStartupScript.of("/data/output", "/data/logs/strelka.log")
                .addCommand(downloadReferenceBam)
                .addCommand(downloadReferenceBai)
                .addCommand(downloadTumorBam)
                .addCommand(downloadTumorBai)
                .addCommand(referenceGenomeDownload)
                .addCommand(configDownload)
                .addCommand(new ConfigureStrelkaWorkflowCommand(downloadTumorBai.getLocalPath(),
                        downloadReferenceBam.getLocalPath(),
                        strelkaConfigFile,
                        referenceGenomeFile));
        computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.somaticCalling(strelkaBash));
        return null;
    }

    @NotNull
    private String localStrelkaConfigFile(final ResourceLocation strelkaConfigLocation, final ResourceDownload strelkaConfigDownload) {
        return strelkaConfigDownload.getLocalPaths()
                .stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("No strelka configuration file was found in resource [%s]",
                        strelkaConfigLocation)));
    }

    @NotNull
    private String localReferenceGenomeFile(final ResourceLocation referenceGenomeLocation,
            final ResourceDownload referenceGenomeDownload) {
        return referenceGenomeDownload.getLocalPaths()
                .stream()
                .filter(file -> file.endsWith("fa") || file.endsWith("fasta"))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("No reference genome fasta file was found in resource [%s]",
                        referenceGenomeLocation)));
    }
}
