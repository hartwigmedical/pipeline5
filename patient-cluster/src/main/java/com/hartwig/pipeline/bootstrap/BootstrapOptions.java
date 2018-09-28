package com.hartwig.pipeline.bootstrap;

import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BootstrapOptions {

    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapOptions.class);
    private static final String PATIENT_FLAG = "p";
    private static final String PATIENT_DIRECTORY_FLAG = "d";
    private static final String VERSION_FLAG = "v";
    private static final String JAR_LIB_FLAG = "l";
    private static final String BUCKET_FLAG = "b";
    private static final String FORCE_JAR_UPLOAD_FLAG = "force_jar_upload";
    private static final String NO_CLEANUP_FLAG = "no_cleanup";
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
    private static final String SBP_SAMPLE_ID_FLAG = "sbp_sample_id";
    private static final String SBP_API_URL_FLAG = "sbp_api_url";
    private static final String SBP_S3_URL_FLAG = "sbp_s3_url";
    private static final String DEFAULT_SBP_API_URL = "http://hmfapi";
    private static final String DEFAULT_SBP_S3_URL = "https://s3.object02.schubergphilis.com";
    private static final String RUN_ID_FLAG = "run_id";
    private static final String NODE_INIT_FLAG = "node_init_script";
    private static final String DEFAULT_NODE_INIT = "node-init.sh";
    private static final String CLOUD_SDK_PATH_FLAG = "cloud_sdk";
    private static final String DEFAULT_CLOUD_SDK_PATH = "/google-cloud-sdk/bin/";
    private static final String CPU_PER_GB_FLAG = "cpu_per_gb";
    private static final String DEFAULT_CPU_PER_GB = "5";

    private static Options options() {
        return new Options().addOption(privateKeyFlag())
                .addOption(patientId())
                .addOption(version())
                .addOption(patientDirectory())
                .addOption(jarLibDirectory())
                .addOption(bucket())
                .addOption(SKIP_UPLOAD_FLAG, false, "Skip uploading of patient data into cloud storeage")
                .addOption(FORCE_JAR_UPLOAD_FLAG, false, "Force upload of JAR even if the version already exists in cloud storage")
                .addOption(NO_CLEANUP_FLAG, false, "Don't delete the cluster or runtime bucket after job is complete")
                .addOption(project())
                .addOption(region())
                .addOption(sbpSampleId())
                .addOption(sbpApiUrl())
                .addOption(sbpS3Url())
                .addOption(runId()).addOption(nodeInitScript()).addOption(cpuPerGB())
                .addOption(gsutilPath());
    }

    private static Option cpuPerGB() {
        return optionWithArgAndDefault(CPU_PER_GB_FLAG, CPU_PER_GB_FLAG, "Number of CPUs to use per GB of FASTQ.", DEFAULT_CPU_PER_GB);
    }

    private static Option gsutilPath() {
        return optionWithArgAndDefault(CLOUD_SDK_PATH_FLAG,
                CLOUD_SDK_PATH_FLAG,
                "Path to the google cloud sdk bin directory (with gsutil and gcloud)",
                DEFAULT_CLOUD_SDK_PATH);
    }

    private static Option nodeInitScript() {
        return optionWithArgAndDefault(NODE_INIT_FLAG,
                NODE_INIT_FLAG,
                "Script to run on initialization of each cluster node. The default script installs BWA",
                DEFAULT_NODE_INIT);
    }

    private static Option runId() {
        return optionWithArg(RUN_ID_FLAG, RUN_ID_FLAG, "Override the generated run id used for runtime bucket and cluster naming", false);
    }

    private static Option sbpApiUrl() {
        return optionWithArgAndDefault(SBP_API_URL_FLAG, "sbp_api_url", "URL of the SBP API endpoint", DEFAULT_SBP_API_URL);
    }

    private static Option sbpS3Url() {
        return optionWithArgAndDefault(SBP_S3_URL_FLAG, "sbp_s3_url", "URL of the SBP S3 endpoint", DEFAULT_SBP_API_URL);
    }

    private static Option sbpSampleId() {
        return optionWithArg(SBP_SAMPLE_ID_FLAG, SBP_SAMPLE_ID_FLAG, "SBP API internal numeric sample id", false);
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
                    .project(commandLine.getOptionValue(PROJECT_FLAG, DEFAULT_PROJECT))
                    .region(handleDashesInRegion(commandLine))
                    .sbpApiUrl(commandLine.getOptionValue(SBP_API_URL_FLAG, DEFAULT_SBP_API_URL))
                    .sbpApiSampleId(sbpApiSampleId(commandLine))
                    .sblS3Url(commandLine.getOptionValue(SBP_S3_URL_FLAG, DEFAULT_SBP_S3_URL))
                    .forceJarUpload(commandLine.hasOption(FORCE_JAR_UPLOAD_FLAG))
                    .noCleanup(commandLine.hasOption(NO_CLEANUP_FLAG))
                    .runId(runId(commandLine))
                    .nodeInitializationScript(commandLine.getOptionValue(NODE_INIT_FLAG, DEFAULT_NODE_INIT))
                    .cpuPerGBRatio(cpuPerGB(commandLine))
                    .cloudSdkPath(commandLine.getOptionValue(CLOUD_SDK_PATH_FLAG, DEFAULT_CLOUD_SDK_PATH))
                    .build());
        } catch (ParseException e) {
            LOGGER.error("Could not parse command line args", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("bootstrap", options());
            return Optional.empty();
        }
    }

    private static int cpuPerGB(final CommandLine commandLine) {
        return Integer.parseInt(commandLine.getOptionValue(CPU_PER_GB_FLAG, DEFAULT_CPU_PER_GB));
    }

    private static Optional<String> runId(final CommandLine commandLine) {
        if (commandLine.hasOption(RUN_ID_FLAG)) {
            return Optional.of(commandLine.getOptionValue(RUN_ID_FLAG));
        }
        return Optional.empty();
    }

    private static Optional<Integer> sbpApiSampleId(final CommandLine commandLine) {
        if (commandLine.hasOption(SBP_SAMPLE_ID_FLAG)) {
            try {
                return Optional.of(Integer.parseInt(commandLine.getOptionValue(SBP_SAMPLE_ID_FLAG)));
            } catch (NumberFormatException e) {
                throw new RuntimeException("SBP API parameter was not a valid ID. This parameter takes the integer IDs which SBP uses in "
                        + "its internal database", e);
            }
        }
        return Optional.empty();
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
    private static Option optionWithArg(final String option, final String name, final String description, final boolean required) {
        Option.Builder builder = Option.builder(option).hasArg().argName(name).desc(description);
        return required ? builder.required().build() : builder.build();
    }
}
