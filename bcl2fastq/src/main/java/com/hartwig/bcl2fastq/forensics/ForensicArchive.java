package com.hartwig.bcl2fastq.forensics;

import com.google.cloud.storage.Bucket;
import com.hartwig.bcl2fastq.Bcl2fastqArguments;
import com.hartwig.pipeline.storage.GSUtil;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class ForensicArchive {

    private final String targetBucket;
    private final Bcl2fastqArguments arguments;

    public ForensicArchive(final String targetBucket, final Bcl2fastqArguments arguments) {
        this.targetBucket = targetBucket;
        this.arguments = arguments;
    }

    public void store(String conversionName, RuntimeBucket runtimeBucket, Bucket inputBucket, String logName) {

        try {
            String targetUrl = "gs://" + targetBucket + "/" + conversionName + "/";
            String outputTarget = targetUrl + "output";
            GSUtil.rsync(arguments.cloudSdkPath(),
                    "gs://" + runtimeBucket.name() + "/",
                    outputTarget,
                    arguments.project(),
                    ".*\\.fastq(\\.gz)?",
                    true);
            GSUtil.rsync(arguments.cloudSdkPath(),
                    "gs://" + inputBucket.getName() + "/" + conversionName + "/",
                    targetUrl + "input",
                    arguments.project(),
                    ".*Logs.*|.*Images.*|.*PeriodicSaveRates.*|.*fastq\\.gz|.*Data.*",
                    true);
            GSUtil.cp(arguments.cloudSdkPath(), logName, outputTarget + "/" + logName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
