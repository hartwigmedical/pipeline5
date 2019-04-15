package com.hartwig.pipeline.calling.germline;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;

import java.util.Calendar;

import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.io.RuntimeBucket;

import static java.lang.String.format;

public class GermlineCaller {
    private static final String OUTPUT_FILENAME = "germline_output.gvcf";

    private final Arguments arguments;
    private final String referenceGenomeBucket;
    private final ComputeEngine executor;
    private final Storage storage;

    GermlineCaller(final Arguments arguments, final ComputeEngine executor, final Storage storage) {
        this.referenceGenomeBucket = arguments.referenceGenomeBucket();
        this.arguments = arguments;
        this.executor = executor;
        this.storage = storage;
    }

    public GermlineCallerOutput run(AlignmentOutput alignmentOutput) {
        String workingDir = "/tmp/hartwig";
        String reference = "Homo_sapiens.GRCh37.GATK.illumina.fasta";

        String timestamp = getTimestamp();
        String outputDir = format("%s/%s", workingDir, timestamp);
        String outputBucketName = format("gatk-germline-output-%s", timestamp);

        GoogleStorageInputOutput inBucket = new GoogleStorageInputOutput(alignmentOutput.finalBamLocation().bucket());
        String jar = "wrappers-local-SNAPSHOT.jar";

        BashStartupScript startupScript = BashStartupScript.of(outputDir, format("%s/run.log", outputDir))
                .addLine("echo Starting up at $(date)")
                .addLine(inBucket.copyToLocal(jar, format("%s/%s", workingDir, jar)))
                .addLine(inBucket.copyToLocal(alignmentOutput.finalBamLocation().path(), workingDir))
                .addLine(inBucket.copyToLocal(format("%s.bai", alignmentOutput.finalBamLocation().path()), workingDir))
                .addLine(new GoogleStorageInputOutput(referenceGenomeBucket).copyToLocal("*", workingDir));

        GatkHaplotypeCaller wrapper = new GatkHaplotypeCaller(format("%s/%s", workingDir, jar),
                format("%s/%s", workingDir, "bam"),
                format("%s/%s", workingDir, reference),
                format("%s/%s", outputDir, OUTPUT_FILENAME));

        GoogleStorageInputOutput outputBucket = new GoogleStorageInputOutput(outputBucketName);
        startupScript.addLine(wrapper.buildCommand())
                .addLine("echo Processing finished at $(date)")
                .addLine(outputBucket.copyFromLocal(format("%s/*", outputDir), ""))
                .addLine(format("date > %s/%s", outputDir, startupScript.completionFlag()))
                .addLine(outputBucket.copyFromLocal(format("%s/%s", outputDir, startupScript.completionFlag()), ""));

        RuntimeBucket bucket = RuntimeBucket.from(storage, arguments.sampleId(), arguments);
        executor.submit(bucket, VirtualMachineJobDefinition.germlineCalling(startupScript));

        return GermlineCallerOutput.of(outputBucketName, OUTPUT_FILENAME);
    }

    private static String getTimestamp() {
        Calendar now = Calendar.getInstance();
        return format("%d%02d%02d_%02d%02d",
                now.get(Calendar.YEAR),
                now.get(Calendar.MONTH) + 1,
                now.get(Calendar.DAY_OF_MONTH),
                now.get(Calendar.HOUR_OF_DAY),
                now.get(Calendar.MINUTE));
    }
}
