package com.hartwig.pipeline.bootstrap;

import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;

class BootstrapOptions {

    private static final String PATIENT_FLAG = "p";
    private static final String PATIENT_DIRECTORY_FLAG = "d";
    private static final String VERSION_FLAG = "v";
    private static final String JAR_LIB_FLAG = "l";
    private static final String BUCKET_FLAG = "b";
    private static final String FORCE_JAR_UPLOAD_FLAG = "force_jar_upload";
    private static final String PROJECT_FLAG = "project";
    private static final String REGION_FLAG = "region";
    private static final String SKIP_UPLOAD_FLAG = "skip_upload";
    private static final String DEFAULT_REGION = "europe-west4";
    private static final String DEFAULT_BUCKET = "pipeline5-runtime";
    private static final String DEFAULT_PROJECT = "hmf-pipeline-development";
    private static final String DEFAULT_VERSION = "";
    private static final String DEFAULT_PATIENT_DIRECTORY = "/patients/";
    private static final String PRIVATE_KEY_FLAG = "k";
    private static final String DEFAULT_JAR_LIB = "/usr/share/pipeline5/";
    private static final String DEFAULT_PRIVATE_KEY_PATH = "/secrets/bootstrap-key.json";

    private static Options options() {
        return new Options().addOption(privateKeyFlag())
                .addOption(patientId())
                .addOption(version())
                .addOption(patientDirectory())
                .addOption(jarLibDirectory())
                .addOption(bucket())
                .addOption(SKIP_UPLOAD_FLAG, false, "Skip uploading of patient data into cloud storeage")
                .addOption(FORCE_JAR_UPLOAD_FLAG, false, "Force upload of JAR even if the version already exists in cloud storage")
                .addOption(project())
                .addOption(region());
    }

    private static Option privateKeyFlag() {
        return optionWithArgAndDefault(PRIVATE_KEY_FLAG,
                "private_key_path",
                "Fully qualified path to the private key for the service account used" + "for all Google Cloud operations",
                DEFAULT_PRIVATE_KEY_PATH);
    }

    private static Option region() {
        return optionWithArgAndDefault(REGION_FLAG, "region", "The region in which to create the cluster.", DEFAULT_REGION);
    }

    private static Option project() {
        return optionWithArgAndDefault(PROJECT_FLAG, "project", "The Google project for which to create the cluster.", DEFAULT_PROJECT);
    }

    private static Option bucket() {
        return optionWithArgAndDefault(BUCKET_FLAG,
                "bucket",
                "Bucket in GS to use for all runtime data. Spark will use this bucket as HDFS.",
                DEFAULT_BUCKET);
    }

    private static Option jarLibDirectory() {
        return optionWithArgAndDefault(JAR_LIB_FLAG,
                "jar_lib_directory",
                "Directory containing the system-{VERSION}.jar.",
                DEFAULT_JAR_LIB);
    }

    private static Option version() {
        return optionWithArgAndDefault(VERSION_FLAG, "version", "Version of pipeline5 to run in spark.", DEFAULT_VERSION);
    }

    @NotNull
    private static Option patientId() {
        return optionWithArgAndDefault(PATIENT_FLAG, "patient", "ID of the patient to process.", "");
    }

    @NotNull
    private static Option patientDirectory() {
        return optionWithArgAndDefault(PATIENT_DIRECTORY_FLAG,
                "patient_directory",
                "Root directory of the patient data",
                DEFAULT_PATIENT_DIRECTORY);
    }

    static Optional<Arguments> from(String[] args) {
        try {
            DefaultParser defaultParser = new DefaultParser();
            CommandLine commandLine = defaultParser.parse(options(), args);
            return Optional.of(Arguments.builder()
                    .privateKeyPath(commandLine.getOptionValue(PRIVATE_KEY_FLAG, DEFAULT_PRIVATE_KEY_PATH))
                    .version(commandLine.getOptionValue(VERSION_FLAG, DEFAULT_VERSION))
                    .patientDirectory(commandLine.getOptionValue(PATIENT_DIRECTORY_FLAG, DEFAULT_PATIENT_DIRECTORY))
                    .patientId(commandLine.getOptionValue(PATIENT_FLAG, ""))
                    .jarLibDirectory(commandLine.getOptionValue(JAR_LIB_FLAG, DEFAULT_JAR_LIB))
                    .runtimeBucket(commandLine.getOptionValue(BUCKET_FLAG, DEFAULT_BUCKET))
                    .forceJarUpload(commandLine.hasOption(FORCE_JAR_UPLOAD_FLAG))
                    .skipPatientUpload(commandLine.hasOption(SKIP_UPLOAD_FLAG))
                    .project(commandLine.getOptionValue(PROJECT_FLAG, DEFAULT_PROJECT))
                    .region(handleDashesInRegion(commandLine))
                    .build());
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("boostrap", options());
            return Optional.empty();
        }
    }

    private static String handleDashesInRegion(final CommandLine commandLine) {
        if (commandLine.hasOption(REGION_FLAG)) {
            return commandLine.getOptionValue(REGION_FLAG);
        }
        return DEFAULT_REGION;
    }

    @NotNull
    private static Option optionWithArgAndDefault(final String option, final String name, final String description,
            final String defaultValue) {
        return optionWithArg(option, name, description + " Default is " + (defaultValue.isEmpty() ? "empty" : defaultValue), false);
    }

    @NotNull
    private static Option optionWithArg(final String option, final String name, final String description) {
        return optionWithArg(option, name, description, true);
    }

    @NotNull
    private static Option optionWithArg(final String option, final String name, final String description, final boolean required) {
        Option.Builder builder = Option.builder(option).hasArg().argName(name).desc(description);
        return required ? builder.required().build() : builder.build();
    }
}
