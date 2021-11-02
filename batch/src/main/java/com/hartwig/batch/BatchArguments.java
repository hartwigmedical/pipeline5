package com.hartwig.batch;

import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;

import com.hartwig.pipeline.CommonArguments;
import com.hartwig.pipeline.resource.RefGenomeVersion;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.immutables.value.Value;

@Value.Immutable
public interface BatchArguments extends CommonArguments {

    String CONCURRENCY = "concurrency";
    String INPUT_FILE = "input_file";
    String OUTPUT_BUCKET = "output_bucket";

    int concurrency();

    String inputFile();

    String command();

    String outputBucket();

    static BatchArguments from(String[] args) {
        try {
            CommandLine commandLine = new DefaultParser().parse(options(), args);
            return ImmutableBatchArguments.builder()
                    .command(args[0])
                    .project(commandLine.getOptionValue(PROJECT, CommonArguments.DEFAULT_DEVELOPMENT_PROJECT))
                    .region(commandLine.getOptionValue(REGION, CommonArguments.DEFAULT_REGION))
                    .useLocalSsds(parseBoolean(commandLine.getOptionValue(LOCAL_SSDS, "true")))
                    .pollInterval(Integer.parseInt(commandLine.getOptionValue(POLL_INTERVAL, "60")))
                    .privateKeyPath(CommonArguments.privateKey(commandLine))
                    .cloudSdkPath(commandLine.getOptionValue(CLOUD_SDK, "/usr/bin"))
                    .serviceAccountEmail(commandLine.getOptionValue(SERVICE_ACCOUNT_EMAIL))
                    .concurrency(Integer.parseInt(commandLine.getOptionValue(CONCURRENCY, "100")))
                    .inputFile(commandLine.getOptionValue(INPUT_FILE))
                    .outputBucket(commandLine.getOptionValue(OUTPUT_BUCKET))
                    .cmek(commandLine.getOptionValue(CMEK, CommonArguments.DEFAULT_DEVELOPMENT_CMEK))
                    .network(commandLine.getOptionValue(PRIVATE_NETWORK, DEFAULT_NETWORK))
                    .refGenomeVersion(RefGenomeVersion.V37)
                    .imageProject("hmf-pipeline-development")
                    .usePreemptibleVms(true)
                    .build();
        } catch (ParseException e) {
            String message = "Failed to parse arguments";
            System.err.println(format("%s: %s", message, e.getMessage()));
            usage();
            System.out.println();
            throw new IllegalArgumentException(message, e);
        }
    }

    private static void usage() {
        System.err.println("\nRecognised options:");
        int padding = 0;
        for (Option option : options().getOptions()) {
            if (option.getOpt().length() > padding) {
                padding = option.getOpt().length();
            }
        }
        final int i = padding;
        options().getOptions().forEach(o -> System.err.println(format("-%-" + i + "s  %s", o.getOpt(), o.getDescription())));
    }

    private static Options options() {
        return new Options().addOption(stringOption(PROJECT, "GCP project"))
                .addOption(stringOption(REGION, "GCP region"))
                .addOption(stringOption(CLOUD_SDK, "Local directory containing gcloud command"))
                .addOption(stringOption(CONCURRENCY, "Limit the number of VMs executing at once to this number"))
                .addOption(stringOption(INPUT_FILE, "Read list of target resources from this inputs file"))
                .addOption(Option.builder(LOCAL_SSDS)
                        .hasArg()
                        .argName("true|false")
                        .desc("Whether to use local SSDs for better performance and lower cost")
                        .build())
                .addOption(stringOption(PRIVATE_KEY_PATH, "Path to JSON file containing GCP credentials"))
                .addOption(stringOption(SERVICE_ACCOUNT_EMAIL, "Email of service account"))
                .addOption(stringOption(OUTPUT_BUCKET, "Output bucket (must exist and must be writable by the service account)"))
                .addOption(stringOption(CMEK, CMEK_DESCRIPTION))
                .addOption(stringOption(PRIVATE_NETWORK, PRIVATE_NETWORK_DESCRIPTION));
    }

    private static Option stringOption(final String option, final String description) {
        return Option.builder(option).hasArg().desc(description).build();
    }

    static ImmutableBatchArguments.Builder builder() {
        return ImmutableBatchArguments.builder();
    }
}
