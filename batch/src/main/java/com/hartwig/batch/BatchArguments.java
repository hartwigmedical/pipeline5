package com.hartwig.batch;

import com.hartwig.pipeline.CommonArguments;
import org.apache.commons.cli.*;
import org.immutables.value.Value;

import static java.lang.Boolean.parseBoolean;

@Value.Immutable
public interface BatchArguments extends CommonArguments {

    String PROJECT = "project";
    String REGION = "region";
    String LOCAL_SSDS = "local_ssds";
    String PREEMPTIBLE_VMS = "preemptible_vms";
    String SERVICE_ACCOUNT_EMAIL = "service_account_email";
    String CLOUD_SDK = "cloud_sdk";
    String CONCURRENCY = "concurrency";
    String INPUT_FILE = "input_file";
    String OUTPUT_BUCKET = "output_bucket";
    String PRIVATE_KEY_PATH = "private_key_path";

    int concurrency();

    String inputFile();

    String command();

    String outputBucket();

    static BatchArguments from(String[] args) {
        try {
            CommandLine commandLine = new DefaultParser().parse(options(), args);
            return ImmutableBatchArguments.builder().command(args[0])
                    .project(commandLine.getOptionValue(PROJECT, "hmf-pipeline-development"))
                    .region(commandLine.getOptionValue(REGION, "europe-west4"))
                    .useLocalSsds(parseBoolean(commandLine.getOptionValue(LOCAL_SSDS, "true")))
                    .usePreemptibleVms(parseBoolean(commandLine.getOptionValue(PREEMPTIBLE_VMS, "true")))
                    .privateKeyPath(commandLine.getOptionValue(PRIVATE_KEY_PATH))
                    .cloudSdkPath(commandLine.getOptionValue(CLOUD_SDK, "/usr/bin"))
                    .serviceAccountEmail(commandLine.getOptionValue(SERVICE_ACCOUNT_EMAIL))
                    .concurrency(Integer.parseInt(commandLine.getOptionValue(CONCURRENCY, "100")))
                    .inputFile(commandLine.getOptionValue(INPUT_FILE)).outputBucket(commandLine.getOptionValue(OUTPUT_BUCKET))
                    .shallow(false)
                    .build();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse arguments", e);
        }
    }

    private static Options options() {
        return new Options().addOption(stringOption(PROJECT, "GCP project"))
                .addOption(stringOption(REGION, "GCP region"))
                .addOption(stringOption(CLOUD_SDK, "Local directory containing gcloud command"))
                .addOption(stringOption(CONCURRENCY, "Limit the number of VMs executing at once to this number"))
                .addOption(stringOption(INPUT_FILE, "Read list of target resources from this input file"))
                .addOption(booleanOption(LOCAL_SSDS, "Whether to use local SSDs for better performance and lower cost"))
                .addOption(booleanOption(PREEMPTIBLE_VMS, "Use pre-emptible VMs to lower cost"))
                .addOption(stringOption(PRIVATE_KEY_PATH, "Path to JSON file containing GCP credentials"))
                .addOption(stringOption(SERVICE_ACCOUNT_EMAIL, "Email of service account"))
                .addOption(stringOption(OUTPUT_BUCKET, "Output bucket (must exist and must be writable by the service account)"));
    }

    private static Option stringOption(final String option, final String description) {
        return Option.builder(option).hasArg().desc(description).build();
    }

    private static Option booleanOption(final String option, final String description) {
        return Option.builder(option).hasArg().argName("true|false").desc(description).build();
    }
}
