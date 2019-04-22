package com.hartwig.pipeline.calling.germline;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.CommandLineOptions;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.storage.StorageProvider;

public class GermlineCallerTest {

    public static final void main(String[] args) throws Exception {
        Arguments arguments = CommandLineOptions.from(args);
        GoogleCredentials credentials = CredentialProvider.from(arguments).get();
        Storage storage = StorageProvider.from(arguments, credentials).get();
        Sample sample = Sample.builder(arguments.sampleDirectory(), arguments.sampleId()).build();
        AlignmentOutput input = AlignmentOutput.of(
                GoogleStorageLocation.of("gatk-germline-bucket", "CPCT12345678R.sorted.bam"),
                GoogleStorageLocation.of("gatk-germline-bucket", "CPCT12345678R.sorted.bam.bai"),
                GoogleStorageLocation.of("unimportant", "whatever"), sample);

        GermlineCallerProvider.from(credentials, storage, arguments).get().run(input);
    }
}