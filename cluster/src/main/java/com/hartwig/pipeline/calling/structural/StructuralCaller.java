package com.hartwig.pipeline.calling.structural;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.calling.structural.gridss.process.IdentifyVariants;
import com.hartwig.pipeline.calling.structural.gridss.stage.Annotation;
import com.hartwig.pipeline.calling.structural.gridss.stage.Assemble;
import com.hartwig.pipeline.calling.structural.gridss.stage.CommandFactory;
import com.hartwig.pipeline.calling.structural.gridss.stage.Preprocess;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.Resource;

public class StructuralCaller {
    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    StructuralCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage,
                     final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public StructuralCallerOutput run(AlignmentPair pair, BamMetricsOutput metricsOutput){
        String tumorSampleName = pair.tumor().sample().name();
        String referenceSampleName = pair.reference().sample().name();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, referenceSampleName, tumorSampleName, arguments);
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        ResourceDownload referenceGenomeDownload = ResourceDownload.from(runtimeBucket,
                new Resource(storage, arguments.referenceGenomeBucket(), runtimeBucket.name()));
        String referenceGenomePath = referenceGenomeDownload.find("fa", "fasta");

        CommandFactory commandFactory = new CommandFactory();

        Preprocess.PreprocessResult preprocessedSample = new Preprocess(commandFactory).initialise("", "", "");
        Preprocess.PreprocessResult preprocessedTumor = new Preprocess(commandFactory).initialise("", "", "");

        Assemble.AssembleResult assemblyResult = new Assemble(commandFactory).initialise(preprocessedSample.svBam(),
                preprocessedTumor.svBam(), referenceGenomePath, "");

        IdentifyVariants calling = commandFactory.buildIdentifyVariants(assemblyResult.svBam(), "", "", referenceGenomePath, "");

        // TODO sampleBam, tumorBam, rawVcf
        Annotation.AnnotationResult annotationResult = new Annotation(commandFactory).initialise("", "", "", "");

        bash.addCommand(referenceGenomeDownload)
                .addLine("echo Preprocessing")
                .addCommands(preprocessedSample.commands())
                .addCommands(preprocessedTumor.commands())
                .addLine("echo Assembly")
                .addCommands(assemblyResult.commands())
                .addLine("echo Calling")
                .addCommand(calling)
                .addLine("echo Annotation")
                .addCommands(annotationResult.commands());

        System.out.println(bash.asUnixString());
        // mark done
        return null;
    }
}
