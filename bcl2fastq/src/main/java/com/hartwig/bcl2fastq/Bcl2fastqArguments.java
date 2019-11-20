package com.hartwig.bcl2fastq;

import static java.lang.Boolean.parseBoolean;

import com.hartwig.pipeline.CommonArguments;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.immutables.value.Value;

@Value.Immutable
public interface Bcl2fastqArguments extends CommonArguments {

    String INPUT_BUCKET = "input_bucket";
    String FLOWCELL = "flowcell";
    String OUTPUT_BUCKET = "output_bucket";

    String inputBucket();

    String outputBucket();

    String flowcell();

    static Bcl2fastqArguments from(String[] args) {
        try {
            CommandLine commandLine = new DefaultParser().parse(options(), args);
            return ImmutableBcl2fastqArguments.builder()
                    .project(commandLine.getOptionValue(PROJECT, "hmf-pipeline-development"))
                    .region(commandLine.getOptionValue(REGION, "europe-west4"))
                    .useLocalSsds(parseBoolean(commandLine.getOptionValue(LOCAL_SSDS, "true")))
                    .usePreemptibleVms(parseBoolean(commandLine.getOptionValue(PREEMPTIBLE_VMS, "true")))
                    .privateKeyPath(commandLine.getOptionValue(PRIVATE_KEY_PATH))
                    .cloudSdkPath(commandLine.getOptionValue(CLOUD_SDK, System.getProperty("user.home") + "/gcloud/google-cloud-sdk/bin"))
                    .serviceAccountEmail(commandLine.getOptionValue(SERVICE_ACCOUNT_EMAIL))
                    .flowcell(commandLine.getOptionValue(FLOWCELL))
                    .inputBucket(commandLine.getOptionValue(INPUT_BUCKET))
                    .outputBucket(commandLine.getOptionValue(OUTPUT_BUCKET))
                    .build();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse arguments", e);
        }
    }

    private static Options options() {
        return new Options().addOption(stringOption(PROJECT, "GCP project"))
                .addOption(stringOption(REGION, "GCP region"))
                .addOption(stringOption(CLOUD_SDK, "Local directory containing gcloud command"))
                .addOption(booleanOption(LOCAL_SSDS, "Whether to use local SSDs for better performance and lower cost"))
                .addOption(booleanOption(PREEMPTIBLE_VMS, "Use pre-emptible VMs to lower cost"))
                .addOption(stringOption(PRIVATE_KEY_PATH, "Path to JSON file containing compute and storage output credentials"))
                .addOption(stringOption(STORAGE_KEY_PATH, "Path to JSON file containing source storage credentials"))
                .addOption(stringOption(SERVICE_ACCOUNT_EMAIL, "Email of service account"))
                .addOption(stringOption(INPUT_BUCKET, "Location of BCL files to convert"))
                .addOption(stringOption(OUTPUT_BUCKET, "Location to persist BCL files once converted"))
                .addOption(stringOption(FLOWCELL, "ID of flowcell from which the BCL files were generated")).addOption(stringOption("sbp_api_url", "URL "));
    }

    private static Option stringOption(final String option, final String description) {
        return Option.builder(option).hasArg().desc(description).build();
    }

    private static Option booleanOption(final String option, final String description) {
        return Option.builder(option).hasArg().argName("true|false").desc(description).build();
    }
}
