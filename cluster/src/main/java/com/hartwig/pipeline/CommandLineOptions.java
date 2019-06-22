package com.hartwig.pipeline;

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
    private static final String VERBOSE_CLOUD_SDK_FLAG = "verbose_cloud_sdk";
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
    private static final String RUN_SNP_GENOTYPER_FLAG = "run_snp_genotyper";
    private static final String RUN_SOMATIC_CALLER_FLAG = "run_somatic_caller";
    private static final String RUN_STRUCTURAL_CALLER_FLAG = "run_structural_caller";
    private static final String RUN_TERTIARY_FLAG = "run_tertiary";
    private static final String TOOLS_BUCKET_FLAG = "tools_bucket";
    private static final String RESOURCE_BUCKET_FLAG = "resource_bucket";
    private static final String PATIENT_REPORT_BUCKET_FLAG = "patient_report_bucket";
    private static final String MODE_FLAG = "mode";
    private static final String SET_ID_FLAG = "set_id";
    private static final String SBP_RUN_ID_FLAG = "sbp_run_id";

    private static Options options() {
        return new Options().addOption(profile())
                .addOption(mode())
                .addOption(privateKey())
                .addOption(version())
                .addOption(sampleDirectory())
                .addOption(sampleId())
                .addOption(setId())
                .addOption(jarLibDirectory())
                .addOption(optionWithBooleanArg(SKIP_UPLOAD_FLAG, "Skip uploading defaultDirectory patient data into cloud storage"))
                .addOption(optionWithBooleanArg(FORCE_JAR_UPLOAD_FLAG,
                        "Force upload defaultDirectory JAR even if the version already exists in cloud storage"))
                .addOption(optionWithBooleanArg(CLEANUP_FLAG, "Don't delete the runtime bucket after job is complete"))
                .addOption(optionWithBooleanArg(USE_PREEMTIBLE_VMS_FLAG,
                        "Do not allocate half the cluster as preemtible VMs to save cost. "
                                + "These VMs can be reclaimed at any time so using this option will make things more stable"))
                .addOption(optionWithBooleanArg(DOWNLOAD_FLAG,
                        "Do not download the final BAM of Google Storage. Will also leave the runtime bucket in place"))
                .addOption(optionWithBooleanArg(VERBOSE_CLOUD_SDK_FLAG,
                        "Have stdout and stderr of Google tools like gsutil stream to the console"))
                .addOption(optionWithBooleanArg(UPLOAD_FLAG,
                        "Don't upload the sample to storage. "
                                + "This should be used in combination with a run_id which points at an existing bucket"))
                .addOption(project())
                .addOption(region())
                .addOption(sbpSampleId())
                .addOption(sbpRunId())
                .addOption(sbpApiUrl())
                .addOption(sbpS3Url())
                .addOption(runId())
                .addOption(nodeInitScript())
                .addOption(gsutilPath())
                .addOption(rclonePath())
                .addOption(rcloneGcpRemote())
                .addOption(rcloneS3Remote())
                .addOption(optionWithBooleanArg(RUN_METRICS_FLAG, "Run wgs metricsOutputFile after BAM creation"))
                .addOption(optionWithBooleanArg(RUN_ALIGNER_FLAG, "Run the aligner on Google Dataproc"))
                .addOption(optionWithBooleanArg(RUN_GERMLINE_CALLER_FLAG, "Run germline calling (gatk) on a VM"))
                .addOption(optionWithBooleanArg(RUN_SOMATIC_CALLER_FLAG, "Run somatic calling (strelka) on a VM"))
                .addOption(optionWithBooleanArg(RUN_STRUCTURAL_CALLER_FLAG, "Run structural calling (gridss) on a VM"))
                .addOption(optionWithBooleanArg(RUN_TERTIARY_FLAG, "Run tertiary analysis algorithms (amber, cobalt, purple)"))
                .addOption(serviceAccountEmail())
                .addOption(resourceBucket())
                .addOption(toolsBucket())
                .addOption(patientReportBucket());
    }

    private static Option sbpRunId() {
        return optionWithArg(SBP_RUN_ID_FLAG, "The id of the run in the SBP API. This will be used to look up set information in the "
                + "somatic pipeline, and to update the correct run on pipeline completion.");
    }

    private static Option setId() {
        return optionWithArg(SET_ID_FLAG,
                "The id of the set for which to run a somatic pipeline. A set represents a valid reference/tumor pair (ie CPCT12345678). "
                        + "Can only be used when mode is somatic and the single sample pipelines have been run for each sample");
    }

    private static Option mode() {
        return optionWithArg(MODE_FLAG, "What mode in which to run the pipeline, single_sample and somatic are supported.");
    }

    private static Option patientReportBucket() {
        return optionWithArg(PATIENT_REPORT_BUCKET_FLAG, "Bucket in which to persist the final patient report and accompanying data.");
    }

    private static Option toolsBucket() {
        return optionWithArg(TOOLS_BUCKET_FLAG, "Bucket containing all common tools (gatk, picard, amber, cobalt, purple, etc");
    }

    private static Option resourceBucket() {
        return optionWithArg(RESOURCE_BUCKET_FLAG, "Bucket containing all common resources (referenceSampleName genome, known indels, pons, etc");
    }

    private static Option profile() {
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

    private static Option gsutilPath() {
        return optionWithArg(CLOUD_SDK_PATH_FLAG, "Path to the google cloud sdk bin directory (with gsutil and gcloud)");
    }

    private static Option nodeInitScript() {
        return optionWithArg(NODE_INIT_FLAG,
                "Script to run on initialization directory each cluster node. The default script installs BWA, sambamba and picard");
    }

    private static Option runId() {
        return optionWithArg(RUN_ID_FLAG, "Override the generated run id used for runtime bucket and cluster naming");
    }

    private static Option sbpApiUrl() {
        return optionWithArg(SBP_API_URL_FLAG, "URL of the SBP API endpoint");
    }

    private static Option sbpS3Url() {
        return optionWithArg(SBP_S3_URL_FLAG, "URL of the SBP S3 endpoint");
    }

    private static Option sbpSampleId() {
        return optionWithArg(SBP_SAMPLE_ID_FLAG, "SBP API internal numeric sample id");
    }

    private static Option privateKey() {
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
        return optionWithArg(VERSION_FLAG, "Version of pipeline5 to run in spark.");
    }

    @NotNull
    private static Option sampleId() {
        return optionWithArg(SAMPLE_ID_FLAG, "ID of the sample for which to process (ie COLO829R, CPCT12345678T)");
    }

    @NotNull
    private static Option sampleDirectory() {
        return optionWithArg(SAMPLE_DIRECTORY_FLAG, "Root directory of the patient data");
    }

    public static Arguments from(String[] args) throws ParseException {
        try {
            DefaultParser defaultParser = new DefaultParser();
            CommandLine commandLine = defaultParser.parse(options(), args);
            Arguments defaults = Arguments.defaults(commandLine.getOptionValue(PROFILE_FLAG, DEFAULT_PROFILE));
            return Arguments.builder()
                    .mode(Arguments.Mode.valueOf(commandLine.getOptionValue(MODE_FLAG, defaults.mode().name()).toUpperCase()))
                    .setId(commandLine.getOptionValue(SET_ID_FLAG, defaults.setId()))
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
                    .sbpApiRunId(sbpRunId(commandLine))
                    .forceJarUpload(booleanOptionWithDefault(commandLine, FORCE_JAR_UPLOAD_FLAG, defaults.forceJarUpload()))
                    .cleanup(booleanOptionWithDefault(commandLine, CLEANUP_FLAG, defaults.cleanup()))
                    .runId(runId(commandLine))
                    .nodeInitializationScript(commandLine.getOptionValue(NODE_INIT_FLAG, defaults.nodeInitializationScript()))
                    .cloudSdkPath(commandLine.getOptionValue(CLOUD_SDK_PATH_FLAG, defaults.cloudSdkPath()))
                    .usePreemptibleVms(booleanOptionWithDefault(commandLine, USE_PREEMTIBLE_VMS_FLAG, defaults.usePreemptibleVms()))
                    .download(booleanOptionWithDefault(commandLine, DOWNLOAD_FLAG, defaults.download()))
                    .upload(booleanOptionWithDefault(commandLine, UPLOAD_FLAG, defaults.upload()))
                    .rclonePath(commandLine.getOptionValue(RCLONE_PATH_FLAG, defaults.rclonePath()))
                    .rcloneGcpRemote(commandLine.getOptionValue(RCLONE_GCP_REMOTE_FLAG, defaults.rcloneGcpRemote()))
                    .rcloneS3Remote(commandLine.getOptionValue(RCLONE_S3_REMOTE_FLAG, defaults.rcloneS3Remote()))
                    .runBamMetrics(booleanOptionWithDefault(commandLine, RUN_METRICS_FLAG, defaults.runBamMetrics()))
                    .runAligner(booleanOptionWithDefault(commandLine, RUN_ALIGNER_FLAG, defaults.runAligner()))
                    .runSnpGenotyper(booleanOptionWithDefault(commandLine, RUN_SNP_GENOTYPER_FLAG, defaults.runSnpGenotyper()))
                    .runGermlineCaller(booleanOptionWithDefault(commandLine, RUN_GERMLINE_CALLER_FLAG, defaults.runGermlineCaller()))
                    .runSomaticCaller(booleanOptionWithDefault(commandLine, RUN_SOMATIC_CALLER_FLAG, defaults.runSomaticCaller()))
                    .runStructuralCaller(booleanOptionWithDefault(commandLine, RUN_STRUCTURAL_CALLER_FLAG, defaults.runStructuralCaller()))
                    .runTertiary(booleanOptionWithDefault(commandLine, RUN_TERTIARY_FLAG, defaults.runTertiary()))
                    .serviceAccountEmail(commandLine.getOptionValue(SERVICE_ACCOUNT_EMAIL_FLAG, defaults.serviceAccountEmail()))
                    .resourceBucket(commandLine.getOptionValue(RESOURCE_BUCKET_FLAG, defaults.resourceBucket()))
                    .toolsBucket(commandLine.getOptionValue(TOOLS_BUCKET_FLAG, defaults.toolsBucket()))
                    .patientReportBucket(commandLine.getOptionValue(PATIENT_REPORT_BUCKET_FLAG, defaults.patientReportBucket()))
                    .profile(defaults.profile())
                    .build();
        } catch (ParseException e) {
            LOGGER.error("Could not parse command line args", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("pipeline5", options());
            throw e;
        }
    }

    private static Optional<Integer> sbpRunId(final CommandLine commandLine) {
        if (commandLine.hasOption(SBP_RUN_ID_FLAG)) {
            try {
                return Optional.of(Integer.parseInt(commandLine.getOptionValue(SBP_RUN_ID_FLAG)));
            } catch (NumberFormatException e) {
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
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
                throw new RuntimeException(e);
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
        return Option.builder(option).hasArg().argName(option).desc(description).build();
    }

    private static Option optionWithBooleanArg(final String option, final String description) {
        return Option.builder(option).hasArg().argName("true|false").desc(description).build();
    }
}
