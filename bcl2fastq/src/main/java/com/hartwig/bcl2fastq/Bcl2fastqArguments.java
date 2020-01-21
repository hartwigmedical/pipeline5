package com.hartwig.bcl2fastq;

import com.hartwig.pipeline.CommonArguments;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.immutables.value.Value;

import static java.lang.Boolean.parseBoolean;

@Value.Immutable
public interface Bcl2fastqArguments extends CommonArguments {

    String ARCHIVE_BUCKET = "archive_bucket";
    String ARCHIVE_PRIVATE_KEY_PATH = "archive_private_key_path";
    String ARCHIVE_PROJECT = "archive_project";
    String FLOWCELL = "flowcell";
    String INPUT_BUCKET = "input_bucket";
    String SBP_API_URL = "sbp_api_url";

    String archiveBucket();

    String archivePrivateKeyPath();

    String archiveProject();

    String inputBucket();

    String flowcell();

    String sbpApiUrl();

    static Bcl2fastqArguments from(String[] args) {
        try {
            CommandLine commandLine = new DefaultParser().parse(options(), args);
            return ImmutableBcl2fastqArguments.builder()
                    .project(commandLine.getOptionValue(PROJECT, "hmf-pipeline-development"))
                    .region(commandLine.getOptionValue(REGION, "europe-west4"))
                    .useLocalSsds(parseBoolean(commandLine.getOptionValue(LOCAL_SSDS, "true")))
                    .usePreemptibleVms(parseBoolean(commandLine.getOptionValue(PREEMPTIBLE_VMS, "true")))
                    .privateKeyPath(CommonArguments.privateKey(commandLine))
                    .cloudSdkPath(commandLine.getOptionValue(CLOUD_SDK, System.getProperty("user.home") + "/gcloud/google-cloud-sdk/bin"))
                    .serviceAccountEmail(commandLine.getOptionValue(SERVICE_ACCOUNT_EMAIL))
                    .flowcell(commandLine.getOptionValue(FLOWCELL))
                    .inputBucket(commandLine.getOptionValue(INPUT_BUCKET))
                    .sbpApiUrl(commandLine.getOptionValue(SBP_API_URL))
                    .archiveBucket(commandLine.getOptionValue(ARCHIVE_BUCKET))
                    .archivePrivateKeyPath(commandLine.getOptionValue(ARCHIVE_PRIVATE_KEY_PATH))
                    .archiveProject(commandLine.getOptionValue(ARCHIVE_PROJECT))
                    .useLocalSsds(false)
                    .usePreemptibleVms(false)
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
                .addOption(stringOption(FLOWCELL, "ID of flowcell from which the BCL files were generated"))
                .addOption(stringOption(SBP_API_URL, "URL of the SBP metadata api"))
                .addOption(stringOption(ARCHIVE_BUCKET, "Bucket to archive to on completion"))
                .addOption(stringOption(ARCHIVE_PRIVATE_KEY_PATH, "Credentials used to perform archiving"))
                .addOption(stringOption(ARCHIVE_PROJECT, "User project for archival"));
    }

    static ImmutableBcl2fastqArguments.Builder builder() {
        return ImmutableBcl2fastqArguments.builder();
    }

    private static Option stringOption(final String option, final String description) {
        return Option.builder(option).hasArg().desc(description).build();
    }

    private static Option booleanOption(final String option, final String description) {
        return Option.builder(option).hasArg().argName("true|false").desc(description).build();
    }

}
