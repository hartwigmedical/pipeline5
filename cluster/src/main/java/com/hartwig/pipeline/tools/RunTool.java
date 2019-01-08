package com.hartwig.pipeline.tools;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.cluster.GoogleDataprocCluster;
import com.hartwig.pipeline.cluster.GoogleStorageJarUpload;
import com.hartwig.pipeline.cluster.JarLocation;
import com.hartwig.pipeline.cluster.NodeInitialization;
import com.hartwig.pipeline.cluster.SparkCluster;
import com.hartwig.pipeline.cluster.SparkJobDefinition;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.performance.ImmutablePerformanceProfile;
import com.hartwig.pipeline.performance.MachineType;
import com.hartwig.pipeline.performance.PerformanceProfile;

public class RunTool {

    private void execute() throws Exception {
        SparkCluster cluster = GoogleDataprocCluster.from(GoogleCredentials.getApplicationDefault(),
                new NodeInitialization("/Users/pwolfe/Code/pipeline2/cluster/src/main/resources/node-init.sh"),
                "test");
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Arguments arguments = Arguments.defaultsBuilder()
                .version("local-SNAPSHOT")
                .jarLibDirectory("/Users/pwolfe/Code/pipeline2/system/target/")
                .build();
        RuntimeBucket bucket = RuntimeBucket.from(storage, "tool", arguments);
        JarLocation jarLocation = new GoogleStorageJarUpload().run(bucket, arguments);
        ImmutablePerformanceProfile performanceProfile =
                PerformanceProfile.builder().numPreemtibleWorkers(6).numPrimaryWorkers(3).master(MachineType.highMemoryWorker()).build();
        cluster.start(performanceProfile, Sample.builder("", "tool").build(), bucket, arguments);
        cluster.submit(SparkJobDefinition.tool(jarLocation, performanceProfile, "com.hartwig.pipeline.runtime.tools.FastQReadCount"),
                arguments);
        cluster.stop(arguments);
    }

    public static void main(String[] args) {
        try {
            new RunTool().execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
