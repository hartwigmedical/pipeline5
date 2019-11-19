package com.hartwig.bcl2fastq;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.storage.StorageProvider;

import org.junit.Test;

public class Bcl2FastqTest {

    @Test
    public void runsBcl2Fastq() throws Exception {
        Arguments arguments = Arguments.defaultsBuilder("development")
                .privateKeyPath("/Users/pwolfe/Code/pipeline5/bootstrap-key.json")
                .useLocalSsds(false)
                .build();
        GoogleCredentials credentials = CredentialProvider.from(arguments).get();
        Storage storage = StorageProvider.from(arguments, credentials).get();
        Bcl2Fastq victim = new Bcl2Fastq(storage,
                ComputeEngine.from(arguments, credentials),
                com.hartwig.bcl2fastq.Arguments.builder().flowcell("tiny").inputBucket("bcl-input").build(),
                ResultsDirectory.defaultDirectory());
        victim.run();
    }
}