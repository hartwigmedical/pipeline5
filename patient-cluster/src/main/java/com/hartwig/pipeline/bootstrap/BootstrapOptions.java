package com.hartwig.pipeline.bootstrap;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.jetbrains.annotations.NotNull;

class BootstrapOptions {

    static final String SKIP_UPLOAD_FLAG = "skip_upload";
    static final String PATIENT_FLAG = "p";
    static final String PATIENT_DIRECTORY_FLAG = "d";

    static Options options() {
        return new Options().addOption(patientId())
                .addOption(patientDirectory())
                .addOption(SKIP_UPLOAD_FLAG, false, "Skip uploading of patient data into cloud storeage");
    }

    @NotNull
    private static Option patientId() {
        return Option.builder(PATIENT_FLAG).hasArg().argName("patient").desc("ID of the patient to process").required().build();
    }

    @NotNull
    private static Option patientDirectory() {
        return Option.builder(PATIENT_DIRECTORY_FLAG)
                .hasArg()
                .argName("patient_directory")
                .desc("Root directory of the patient data")
                .required()
                .build();
    }
}
