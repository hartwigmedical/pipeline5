package com.hartwig.pipeline;

import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class CommandLineOptions {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineOptions.class);
    private static final String SAMPLE_DIRECTORY_FLAG = "sample_directory";
    private static final String VERSION_FLAG = "version";
    private static final String JAR_DIRECTORY_FLAG = "jar";
    private static final String FORCE_JAR_UPLOAD_FLAG = "force_jar_upload";
    private static final String CLEANUP_FLAG = "cleanup";
    private static final String UPLOAD_FLAG = "upload";
    private static final String PROJECT_FLAG = "project";
    private static final String REGION_FLAG = "region";
    private static final String SKIP_UPLOAD_FLAG = "skip_upload";
    private static final String PRIVATE_KEY_FLAG = "private_key_path";
    private static final String SBP_SAMPLE_ID_FLAG = "sbp_sample_id";
    private static final String SBP_API_URL_FLAG = "sbp_api_url";
    private static final String SBP_S3_URL_FLAG = "sbp_s3_url";
    private static final String RUN_ID_FLAG = "run_id";
    private static final String NODE_INIT_FLAG = "node_init_script";
    private static final String CLOUD_SDK_PATH_FLAG = "cloud_sdk";
    private static final String USE_PREEMTIBLE_VMS_FLAG = "preemtible_vms";
    private static final String DOWNLOAD_FLAG = "download";
    private static final String REFERENCE_GENOME_BUCKET_FLAG = "reference_genome";
    private static final String VERBOSE_CLOUD_SDK_FLAG = "verbose_cloud_sdk";
    private static final String KNOWN_INDELS_BUCKET_FLAG = "known_indels";
    private static final String KNOWN_SNPS_BUCKET_FLAG = "known_snps";
    private static final String RCLONE_PATH_FLAG = "rclone_path";
    private static final String RCLONE_GCP_REMOTE_FLAG = "rclone_gcp_remote";
    private static final String RCLONE_S3_REMOTE_FLAG = "rclone_s3_remote";
    private static final String RUN_METRICS_FLAG = "run_bam_metrics";
    private static final String PROFILE_FLAG = "profile";
    private static final String SAMPLE_ID_FLAG = "sample_id";
    private static final String SERVICE_ACCOUNT_EMAIL_FLAG = "service_account_email";

    private static final String DEFAULT_PROFILE = "production";
    private static final String RUN_ALIGNER_FLAG = "run_aligner";
    private static final String RUN_GERMLINE_CALLER_FLAG = "run_germline_caller";
    private static final String RUN_SOMATIC_CALLER_FLAG = "run_somatic_caller";
    private static final String RUN_STRUCTURAL_CALLER_FLAG = "run_structural_caller";
    private static final String RUN_TERTIARY_FLAG = "run_tertiary";

    private static Options options() {
        return new Options().addOption(profileFlag())
                .addOption(privateKeyFlag())
                .addOption(version())
                .addOption(sampleDirectory())
                .addOption(sampleId())
                .addOption(jarLibDirectory())
                .addOption(optionWithBooleanArg(SKIP_UPLOAD_FLAG, "Skip uploading defaultDirectory patient data into cloud storage"))
                .addOption(optionWithBooleanArg(FORCE_JAR_UPLOAD_FLAG,
                        "Force upload defaultDirectory JAR even if the version already exists in cloud storage"))
                .addOption(optionWithBooleanArg(CLEANUP_FLAG, "Don't delete the runtime bucket after job is complete"))
                .addOption(optionWithBooleanArg(USE_PREEMTIBLE_VMS_FLAG,
                        "Do not allocate half the cluster as preemtible VMs to save cost. "
                                + "These VMs can be reclaimed at any time so using this option will make things more stable"))
                .addOption(optionWithBooleanArg(DOWNLOAD_FLAG,
                        "Do not download the final BAM from Google Storage. Will also leave the runtime bucket in place"))
                .addOption(optionWithBooleanArg(VERBOSE_CLOUD_SDK_FLAG,
                        "Have stdout and stderr from Google tools like gsutil strem to the console"))
                .addOption(optionWithBooleanArg(UPLOAD_FLAG,
                        "Don't upload the sample to storage. "
                                + "This should be used in combination with a run_id which points at an existing bucket"))
                .addOption(project())
                .addOption(region())
                .addOption(sbpSampleId())
                .addOption(sbpApiUrl())
                .addOption(sbpS3Url())
                .addOption(runId())
                .addOption(nodeInitScript())
                .addOption(gsutilPath())
                .addOption(referenceGenomeBucket())
                .addOption(knownIndelsBucket())
                .addOption(rclonePath())
                .addOption(rcloneGcpRemote())
                .addOption(rcloneS3Remote())
                .addOption(optionWithBooleanArg(RUN_METRICS_FLAG, "Run wgs metrics affter BAM creation"))
                .addOption(optionWithBooleanArg(RUN_ALIGNER_FLAG, "Run the aligner on Google Dataproc"))
                .addOption(optionWithBooleanArg(RUN_GERMLINE_CALLER_FLAG, "Run germline calling (gatk) on a VM"))
                .addOption(optionWithBooleanArg(RUN_SOMATIC_CALLER_FLAG, "Run somatic calling (strelka) on a VM"))
                .addOption(optionWithBooleanArg(RUN_STRUCTURAL_CALLER_FLAG, "Run structural calling (gridss) on a VM"))
                .addOption(optionWithBooleanArg(RUN_TERTIARY_FLAG, "Run tertiary analysis algorithms (amber, cobalt, purple)"))
                .addOption(serviceAccountEmail());
    }

    private static Option profileFlag() {
        return optionWithArg(PROFILE_FLAG, "Defaults profile to use. Accepts [production|development]");
    }

    private static Option rclonePath() {
        return optionWithArg(RCLONE_PATH_FLAG, "Path to rclone binary directory");
    }

    private static Option rcloneGcpRemote() {
        return optionWithArg(RCLONE_GCP_REMOTE_FLAG, "RClone remote to use for Google Storage (upload fastqs and download bams)");
    }

    private static Option rcloneS3Remote() {
        return optionWithArg(RCLONE_S3_REMOTE_FLAG, "RClone remote to use for AWS " + "(download fastqs and upload bams)");
    }

    private static Option knownIndelsBucket() {
        return optionWithArg(KNOWN_INDELS_BUCKET_FLAG,
                "Bucket from which to copy the known indel sites VCFs into the runtime bucket. Just a name, not a url (no gs://)");
    }

    private static Option referenceGenomeBucket() {
        return optionWithArg(REFERENCE_GENOME_BUCKET_FLAG,
                "Bucket from which to copy the reference " + "genome into the runtime bucket. Just a name, not a url (no gs://)");
    }

    private static Option gsutilPath() {
        return optionWithArg(CLOUD_SDK_PATH_FLAG, "Path to the google cloud sdk bin directory (with gsutil and gcloud)");
    }

    private static Option nodeInitScript() {
        return optionWithArg(NODE_INIT_FLAG,
                "Script to run on initialization defaultDirectory each cluster node. The default script installs BWA, sambamba and picard");
    }

    private static Option runId() {
        return optionWithArg(RUN_ID_FLAG, "Override the generated run id used for runtime bucket and cluster naming");
    }

    private static Option sbpApiUrl() {
        return optionWithArg(SBP_API_URL_FLAG, "URL defaultDirectory the SBP API endpoint");
    }

    private static Option sbpS3Url() {
        return optionWithArg(SBP_S3_URL_FLAG, "URL defaultDirectory the SBP S3 endpoint");
    }

    private static Option sbpSampleId() {
        return optionWithArg(SBP_SAMPLE_ID_FLAG, "SBP API internal numeric sample id");
    }

    private static Option privateKeyFlag() {
        return optionWithArg(PRIVATE_KEY_FLAG,
                "Fully qualified path to the private key for the service account used for all Google Cloud operations");
    }

    private static Option serviceAccountEmail() {
        return optionWithArg(SERVICE_ACCOUNT_EMAIL_FLAG, "Service account associated with the private key");
    }

    private static Option region() {
        return optionWithArg(REGION_FLAG, "The region in which to get the cluster.");
    }

    private static Option project() {
        return optionWithArg(PROJECT_FLAG, "The Google project for which to get the cluster.");
    }

    private static Option jarLibDirectory() {
        return optionWithArg(JAR_DIRECTORY_FLAG, "Directory containing the system-{VERSION}.jar.");
    }

    private static Option version() {
        return optionWithArg(VERSION_FLAG, "Version defaultDirectory pipeline5 to run in spark.");
    }

    @NotNull
    private static Option sampleId() {
        return optionWithArg(SAMPLE_ID_FLAG, "Full path to the fastq files to process", true);
    }

    @NotNull
    private static Option sampleDirectory() {
        return optionWithArg(SAMPLE_DIRECTORY_FLAG, "Root directory defaultDirectory the patient data");
    }

    public static Arguments from(String[] args) throws ParseException {
        try {
            DefaultParser defaultParser = new DefaultParser();
            CommandLine commandLine = defaultParser.parse(options(), args);
            Arguments defaults = Arguments.defaults(commandLine.getOptionValue(PROFILE_FLAG, DEFAULT_PROFILE));
            return Arguments.builder()
                    .privateKeyPath(commandLine.getOptionValue(PRIVATE_KEY_FLAG, defaults.privateKeyPath()))
                    .version(commandLine.getOptionValue(VERSION_FLAG, defaults.version()))
                    .sampleDirectory(commandLine.getOptionValue(SAMPLE_DIRECTORY_FLAG, defaults.sampleDirectory()))
                    .sampleId(commandLine.getOptionValue(SAMPLE_ID_FLAG, defaults.sampleId()))
                    .jarDirectory(commandLine.getOptionValue(JAR_DIRECTORY_FLAG, defaults.jarDirectory()))
                    .project(commandLine.getOptionValue(PROJECT_FLAG, defaults.project()))
                    .region(handleDashesInRegion(commandLine, defaults.region()))
                    .sbpApiUrl(commandLine.getOptionValue(SBP_API_URL_FLAG, defaults.sbpApiUrl()))
                    .sbpApiSampleId(sbpApiSampleId(commandLine))
                    .sbpS3Url(commandLine.getOptionValue(SBP_S3_URL_FLAG, defaults.sbpS3Url()))
                    .forceJarUpload(booleanOptionWithDefault(commandLine, FORCE_JAR_UPLOAD_FLAG, defaults.forceJarUpload()))
                    .cleanup(booleanOptionWithDefault(commandLine, CLEANUP_FLAG, defaults.cleanup()))
                    .runId(runId(commandLine))
                    .nodeInitializationScript(commandLine.getOptionValue(NODE_INIT_FLAG, defaults.nodeInitializationScript()))
                    .cloudSdkPath(commandLine.getOptionValue(CLOUD_SDK_PATH_FLAG, defaults.cloudSdkPath()))
                    .usePreemptibleVms(booleanOptionWithDefault(commandLine, USE_PREEMTIBLE_VMS_FLAG, defaults.usePreemptibleVms()))
                    .download(booleanOptionWithDefault(commandLine, DOWNLOAD_FLAG, defaults.download()))
                    .referenceGenomeBucket(commandLine.getOptionValue(REFERENCE_GENOME_BUCKET_FLAG, defaults.referenceGenomeBucket()))
                    .knownIndelsBucket(commandLine.getOptionValue(KNOWN_INDELS_BUCKET_FLAG, defaults.knownIndelsBucket()))
                    .knownSnpsBucket(commandLine.getOptionValue(KNOWN_SNPS_BUCKET_FLAG, defaults.knownSnpsBucket()))
                    .upload(booleanOptionWithDefault(commandLine, UPLOAD_FLAG, defaults.upload()))
                    .rclonePath(commandLine.getOptionValue(RCLONE_PATH_FLAG, defaults.rclonePath()))
                    .rcloneGcpRemote(commandLine.getOptionValue(RCLONE_GCP_REMOTE_FLAG, defaults.rcloneGcpRemote()))
                    .rcloneS3Remote(commandLine.getOptionValue(RCLONE_S3_REMOTE_FLAG, defaults.rcloneS3Remote()))
                    .runBamMetrics(booleanOptionWithDefault(commandLine, RUN_METRICS_FLAG, defaults.runBamMetrics()))
                    .runAligner(booleanOptionWithDefault(commandLine, RUN_ALIGNER_FLAG, defaults.runAligner()))
                    .runGermlineCaller(booleanOptionWithDefault(commandLine, RUN_GERMLINE_CALLER_FLAG, defaults.runGermlineCaller()))
                    .runSomaticCaller(booleanOptionWithDefault(commandLine, RUN_SOMATIC_CALLER_FLAG, defaults.runSomaticCaller()))
                    .runStructuralCaller(booleanOptionWithDefault(commandLine, RUN_STRUCTURAL_CALLER_FLAG, defaults.runStructuralCaller()))
                    .runTertiary(booleanOptionWithDefault(commandLine, RUN_TERTIARY_FLAG, defaults.runTertiary()))
                    .serviceAccountEmail(commandLine.getOptionValue(SERVICE_ACCOUNT_EMAIL_FLAG, defaults.serviceAccountEmail()))
                    .profile(defaults.profile())
                    .build();
        } catch (ParseException e) {
            LOGGER.error("Could not parse command line args", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("bootstrap", options());
            throw e;
        }
    }

    private static boolean booleanOptionWithDefault(final CommandLine commandLine, final String flag, final boolean defaultValue)
            throws ParseException {
        String value = commandLine.getOptionValue(flag, Boolean.toString(defaultValue));
        if (!value.equals("true") && !value.equals("false")) {
            throw new ParseException(flag + " is a flag and only accepts true|false");
        }
        return Boolean.valueOf(value);
    }

    private static Optional<String> runId(CommandLine commandLine) {
        if (commandLine.hasOption(RUN_ID_FLAG)) {
            return Optional.of(commandLine.getOptionValue(RUN_ID_FLAG));
        }
        return Optional.empty();
    }

    private static Optional<Integer> sbpApiSampleId(CommandLine commandLine) {
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

    private static String handleDashesInRegion(final CommandLine commandLine, final String defaultRegion) {
        if (commandLine.hasOption(REGION_FLAG)) {
            return commandLine.getOptionValue(REGION_FLAG);
        }
        return defaultRegion;
    }

    private static Option optionWithArg(final String option, final String description) {
        return optionWithArg(option, description, false);
    }

    private static Option optionWithArg(final String option, final String description, final boolean required) {
        Option.Builder builder = Option.builder(option).hasArg().argName(option).desc(description);
        return required ? builder.required().build() : builder.build();
    }

    private static Option optionWithBooleanArg(final String option, final String description) {
        return Option.builder(option).hasArg().argName("true|false").desc(description).build();
    }
}
