package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.cluster.vm.BashStartupScript;
import com.hartwig.pipeline.cluster.vm.GatkHaplotypeCaller;
import com.hartwig.pipeline.cluster.vm.GoogleStorage;
import com.hartwig.pipeline.cluster.vm.GoogleVirtualMachine;

import java.util.Calendar;

import static java.lang.String.format;

public class GermlineCaller {
    private static final String OUTPUT_FILENAME = "germline_output.gvcf";

    private final Arguments arguments;
    private String referenceGenomeBucket;

    GermlineCaller(final Arguments arguments) {
        this.referenceGenomeBucket = arguments.referenceGenomeBucket();
        this.arguments = arguments;
    }

    public GermlineCallerOutput run(AlignmentOutput alignmentOutput) {
        String workingDir = "/tmp/hartwig";
        String reference = "Homo_sapiens.GRCh37.GATK.illumina.fasta";

        String timestamp = getTimestamp();
        String outputDir = format("%s/%s", workingDir, timestamp);
        String outputBucketName = format("gatk-germline-output-%s", timestamp);

        GoogleStorage inBucket = new GoogleStorage(alignmentOutput.finalBamLocation().bucket());
        String jar = "wrappers-local-SNAPSHOT.jar";

        BashStartupScript startupScript = BashStartupScript.bashBuilder()
                .outputToDir(outputDir)
                .logToFile(format("%s/run.log", outputDir))
                .addLine("echo Starting up at $(date)")
                .addLine(inBucket.copyToLocal(jar, format("%s/%s", workingDir, jar)))
                .addLine(inBucket.copyToLocal(alignmentOutput.finalBamLocation().path(), workingDir))
                .addLine(inBucket.copyToLocal(format("%s.bai", alignmentOutput.finalBamLocation().path()), workingDir))
                .addLine(new GoogleStorage(referenceGenomeBucket).copyToLocal("*", workingDir));

        GatkHaplotypeCaller wrapper = new GatkHaplotypeCaller(format("%s/%s", workingDir, jar),
                format("%s/%s", workingDir, "bam"),
                format("%s/%s", workingDir, reference), format("%s/%s", outputDir, OUTPUT_FILENAME));

        GoogleStorage outputBucket = new GoogleStorage(outputBucketName);
        startupScript.addLine(wrapper.buildCommand())
                .addLine("echo Processing finished at $(date)")
                .addLine(outputBucket.copyFromLocal(format("%s/*", outputDir), ""))
                .addLine(format("date > %s/%s", outputDir, startupScript.completionFlag()))
                .addLine(outputBucket.copyFromLocal(format("%s/%s", outputDir, startupScript.completionFlag()), ""));

        GoogleVirtualMachine.germline(arguments, startupScript, outputBucketName).run();

        return GermlineCallerOutput.of(outputBucketName, OUTPUT_FILENAME);
    }

    private static String getTimestamp() {
        Calendar now = Calendar.getInstance();
        return format("%d%02d%02d_%02d%02d", now.get(Calendar.YEAR),
                now.get(Calendar.MONTH ) + 1,
                now.get(Calendar.DAY_OF_MONTH),
                now.get(Calendar.HOUR_OF_DAY),
                now.get(Calendar.MINUTE));
    }
}
