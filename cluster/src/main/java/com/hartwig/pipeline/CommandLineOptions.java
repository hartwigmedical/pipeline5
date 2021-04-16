package com.hartwig.pipeline;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.pipeline.Arguments.DefaultsProfile;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.tools.Versions;

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
    private static final String CLEANUP_FLAG = "cleanup";
    private static final String PROJECT_FLAG = "project";
    private static final String REGION_FLAG = "region";
    private static final String PRIVATE_KEY_FLAG = "private_key_path";
    private static final String SBP_API_URL_FLAG = "sbp_api_url";
    private static final String SBP_S3_URL_FLAG = "sbp_s3_url";
    private static final String RUN_ID_FLAG = "run_id";
    private static final String CLOUD_SDK_PATH_FLAG = "cloud_sdk";
    private static final String USE_PREEMTIBLE_VMS_FLAG = "preemptible_vms";
    private static final String USE_LOCAL_SSDS_FLAG = "local_ssds";
    private static final String DOWNLOAD_FLAG = "download";
    private static final String VERBOSE_CLOUD_SDK_FLAG = "verbose_cloud_sdk";
    private static final String RCLONE_PATH_FLAG = "rclone_path";
    private static final String RCLONE_GCP_REMOTE_FLAG = "rclone_gcp_remote";
    private static final String RCLONE_S3_REMOTE_DOWNLOAD_FLAG = "rclone_s3_remote_download";
    private static final String RCLONE_S3_REMOTE_UPLOAD_FLAG = "rclone_s3_remote_upload";
    private static final String RUN_METRICS_FLAG = "run_bam_metrics";
    private static final String PROFILE_FLAG = "profile";
    private static final String SERVICE_ACCOUNT_EMAIL_FLAG = "service_account_email";
    private static final String MAX_CONCURRENT_LANES_FLAG = "max_concurrent_lanes";
    private static final String DEFAULT_PROFILE = "production";
    private static final String OUTPUT_CRAM_FLAG = "output_cram";
    private static final String PUBLISH_TO_TURQUOISE_FLAG = "publish_to_turquoise";
    private static final String RUN_GERMLINE_CALLER_FLAG = "run_germline_caller";
    private static final String RUN_SNP_GENOTYPER_FLAG = "run_snp_genotyper";
    private static final String RUN_SOMATIC_CALLER_FLAG = "run_somatic_caller";
    private static final String RUN_SAGE_GERMLINE_CALLER_FLAG = "run_sage_germline_caller";
    private static final String RUN_STRUCTURAL_CALLER_FLAG = "run_structural_caller";
    private static final String RUN_TERTIARY_FLAG = "run_tertiary";
    private static final String RUN_HLA_TYPING_FLAG = "run_hla_typing";
    private static final String OUTPUT_BUCKET_FLAG = "output_bucket";
    private static final String ARCHIVE_BUCKET_FLAG = "archive_bucket";
    private static final String ARCHIVE_PROJECT_FLAG = "archive_project";
    private static final String ARCHIVE_PRIVATE_KEY_FLAG = "archive_private_key_path";
    private static final String UPLOAD_PRIVATE_KEY_FLAG = "upload_private_key_path";
    private static final String SET_ID_FLAG = "set_id";
    private static final String SBP_RUN_ID_FLAG = "sbp_run_id";
    private static final String NETWORK_FLAG = "network";
    private static final String SUBNET_FLAG = "subnet";
    private static final String NETWORK_TAGS_FLAG = "network_tags";
    private static final String CMEK_FLAG = "cmek";
    private static final String SHALLOW_FLAG = "shallow";
    private static final String ZONE_FLAG = "zone";
    private static final String REF_GENOME_VERSION_FLAG = "ref_genome_version";
    private static final String REF_GENOME_URL = "ref_genome_url";
    private static final String SAMPLE_JSON_FLAG = "sample_json";
    private static final String STARTING_POINT_FLAG = "starting_point";
    private static final String IMAGE_NAME_FLAG = "image_name";
    private static final String BIOPSY_FLAG = "biopsy";
    private static final String IMAGE_PROJECT_FLAG = "image_project";
    private static final String USE_CRAMS_FLAG = "use_crams";
    private static final String PUBSUB_PROJECT_FLAG = "pubsub_project";

    private static Options options() {
        return new Options().addOption(profile())
                .addOption(privateKey())
                .addOption(sampleDirectory())
                .addOption(setId())
                .addOption(optionWithBooleanArg(CLEANUP_FLAG, "Don't delete the runtime bucket after job is complete"))
                .addOption(optionWithBooleanArg(USE_PREEMTIBLE_VMS_FLAG,
                        "Use preemtible VMs to lower cost. "
                                + "These VMs can be reclaimed at any time so using them can make the run less stable."))
                .addOption(optionWithBooleanArg(USE_LOCAL_SSDS_FLAG,
                        "Use local (ephemeral) SSDs rather than persistent storage to lower cost and improve performance. "
                                + "VMs started with these devices can only be started once so may not be suitable for development"))
                .addOption(optionWithBooleanArg(DOWNLOAD_FLAG,
                        "Do not download the final BAM of Google Storage. Will also leave the runtime bucket in place"))
                .addOption(optionWithBooleanArg(VERBOSE_CLOUD_SDK_FLAG,
                        "Have stdout and stderr of Google tools like gsutil stream to the console"))
                .addOption(project())
                .addOption(region())
                .addOption(sbpRunId())
                .addOption(sbpApiUrl())
                .addOption(sbpS3Url())
                .addOption(runId())
                .addOption(gsutilPath())
                .addOption(rclonePath())
                .addOption(rcloneGcpRemote())
                .addOption(rcloneS3RemoteDownload())
                .addOption(rcloneS3RemoteUpload())
                .addOption(optionWithBooleanArg(RUN_METRICS_FLAG, "Run wgs metricsOutputFile after BAM creation"))
                .addOption(optionWithBooleanArg(RUN_GERMLINE_CALLER_FLAG, "Run germline calling (gatk) on a VM"))
                .addOption(optionWithBooleanArg(RUN_SOMATIC_CALLER_FLAG, "Run somatic calling (sage) on a VM"))
                .addOption(optionWithBooleanArg(RUN_SAGE_GERMLINE_CALLER_FLAG, "Run sage germline calling on a VM"))
                .addOption(optionWithBooleanArg(RUN_STRUCTURAL_CALLER_FLAG, "Run structural calling (gridss) on a VM"))
                .addOption(optionWithBooleanArg(RUN_TERTIARY_FLAG, "Run tertiary analysis algorithms (amber, cobalt, purple)"))
                .addOption(optionWithBooleanArg(RUN_SNP_GENOTYPER_FLAG, "Run snp genotyper for QC against genotyping"))
                .addOption(optionWithBooleanArg(RUN_HLA_TYPING_FLAG, "Run HLA typing (lilac) on a VM"))
                .addOption(serviceAccountEmail())
                .addOption(patientReportBucket())
                .addOption(archiveBucket())
                .addOption(archiveProject())
                .addOption(archivePrivateKey())
                .addOption(uploadPrivateKey())
                .addOption(network())
                .addOption(subnet())
                .addOption(networkTags())
                .addOption(cmek())
                .addOption(optionWithBooleanArg(SHALLOW_FLAG,
                        "Run with ShallowSeq configuration.Germline and health checker are disabled and purple is run with low coverage "
                                + "options."))
                .addOption(optionWithBooleanArg(OUTPUT_CRAM_FLAG, "Produce CRAM rather than BAM files"))
                .addOption(optionWithBooleanArg(PUBLISH_TO_TURQUOISE_FLAG, "Publish events on pipeline start and stop to turquoise."))
                .addOption(optionWithArg(CommonArguments.POLL_INTERVAL,
                        "Time in seconds between status checks against GCP. "
                                + "Increase to allow more concurrent VMs to run at the expense of state change detection resolution."))
                .addOption(zone())
                .addOption(refGenomeVersion())
                .addOption(refGenomeUrl())
                .addOption(maxConcurrentLanes())
                .addOption(json())
                .addOption(startingPoint())
                .addOption(imageName())
                .addOption(biopsy())
                .addOption(imageProject())
                .addOption(useCrams())
                .addOption(pubsubProject());
    }

    private static Option pubsubProject() {
        return optionWithArg(PUBSUB_PROJECT_FLAG, "Project to publish pipeline events over pub/sub");
    }

    private static Option useCrams() {
        return optionWithArg(USE_CRAMS_FLAG, "Don't convert cram back to bam before running downstream stages");
    }

    private static Option imageProject() {
        return optionWithArg(IMAGE_PROJECT_FLAG, "Project in which the source image is located");
    }

    private static Option biopsy() {
        return optionWithArg(BIOPSY_FLAG, "Name of the biopsy registered in the API.");
    }

    private static Option refGenomeUrl() {
        return optionWithArg(REF_GENOME_URL, "GCS url to a valid reference genome fasta file (ie. gs://bucket/genome.fasta)");
    }

    private static Option startingPoint() {
        return optionWithArg(STARTING_POINT_FLAG,
                "Run from a starting point after fastq alignment. Supporting starting points are (alignment_complete, calling_complete, "
                        + "purple_complete)");
    }

    private static Option json() {
        return optionWithArg(SAMPLE_JSON_FLAG, "JSON file defining the location of FASTQ inputs in GCP.");
    }

    private static Option imageName() {
        return optionWithArg(IMAGE_NAME_FLAG, String.format("Image to use instead of the latest %s image", Versions.imageVersion()));
    }

    private static Option maxConcurrentLanes() {
        return optionWithArg(MAX_CONCURRENT_LANES_FLAG,
                "The max number of lanes to align concurrently. This option can be used to throttle"
                        + "the amount of CPUs used during alignment for samples with a large number of lanes.");
    }

    private static Option cmek() {
        return optionWithArg(CMEK_FLAG,
                "The name of the Customer Managed Encryption Key. When this flag is populated all runtime buckets will use this key.");
    }

    private static Option network() {
        return optionWithArg(NETWORK_FLAG,
                "The name of the network to use. Ensure the network has been created in GCP before enabling this flag");
    }

    private static Option subnet() {
        return optionWithArg(SUBNET_FLAG,
                "The name of the subnetwork to use. Ensure the subnetwork has been created in GCP before enabling this flag");
    }

    private static Option networkTags() {
        return optionWithArg(NETWORK_TAGS_FLAG, "Network tags to apply to all GCE instances, comma delimited.");
    }

    private static Option sbpRunId() {
        return optionWithArg(SBP_RUN_ID_FLAG,
                "The id of the run in the SBP API. This will be used to look up set information in the "
                        + "somatic pipeline, and to update the correct run on pipeline completion.");
    }

    private static Option setId() {
        return optionWithArg(SET_ID_FLAG,
                "The id of the set for which to run a somatic pipeline. A set represents a valid reference/tumor pair (ie CPCT12345678). "
                        + "Can only be used when mode is somatic and the single sample pipelines have been run for each sample");
    }

    private static Option patientReportBucket() {
        return optionWithArg(OUTPUT_BUCKET_FLAG, "Bucket in which to persist the final patient report and accompanying data.");
    }

    private static Option archiveBucket() {
        return optionWithArg(ARCHIVE_BUCKET_FLAG, "Bucket to use for data request archival.");
    }

    private static Option archiveProject() {
        return optionWithArg(ARCHIVE_PROJECT_FLAG, "Project to use for data request archival.");
    }

    private static Option uploadPrivateKey() {
        return optionWithArg(UPLOAD_PRIVATE_KEY_FLAG, "Private key to use for upload of fastq files");
    }

    private static Option archivePrivateKey() {
        return optionWithArg(ARCHIVE_PRIVATE_KEY_FLAG, "Private key to use for data request archival.");
    }

    private static Option profile() {
        return optionWithArg(PROFILE_FLAG,
                format("Defaults profile to use. Accepts [%s]",
                        Arrays.stream(DefaultsProfile.values()).map(Enum::name).collect(Collectors.joining("|"))));
    }

    private static Option rclonePath() {
        return optionWithArg(RCLONE_PATH_FLAG, "Path to rclone binary directory");
    }

    private static Option rcloneGcpRemote() {
        return optionWithArg(RCLONE_GCP_REMOTE_FLAG, "RClone remote to use for Google Storage (upload fastqs and download outputs)");
    }

    private static Option rcloneS3RemoteDownload() {
        return optionWithArg(RCLONE_S3_REMOTE_DOWNLOAD_FLAG, "RClone remote to use for AWS (download fastqs)");
    }

    private static Option rcloneS3RemoteUpload() {
        return optionWithArg(RCLONE_S3_REMOTE_UPLOAD_FLAG, "RClone remote to use for AWS (upload outputs)");
    }

    private static Option gsutilPath() {
        return optionWithArg(CLOUD_SDK_PATH_FLAG, "Path to the google cloud sdk bin directory (with gsutil and gcloud)");
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

    private static Option zone() {
        return optionWithArg(ZONE_FLAG, "The zone for which to get the clusters.");
    }

    private static Option refGenomeVersion() {
        return optionWithArg(REF_GENOME_VERSION_FLAG,
                format("Ref genome version, default [%s], values [%s]",
                        RefGenomeVersion.V37.pipeline(),
                        refGenomeVersions().collect(Collectors.joining(","))));
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
                    .setId(commandLine.getOptionValue(SET_ID_FLAG, defaults.setId()))
                    .privateKeyPath(CommonArguments.privateKey(commandLine).or(defaults::privateKeyPath))
                    .project(commandLine.getOptionValue(PROJECT_FLAG, defaults.project()))
                    .region(handleDashesInRegion(commandLine, defaults.region()))
                    .sbpApiUrl(commandLine.getOptionValue(SBP_API_URL_FLAG, defaults.sbpApiUrl()))
                    .sbpApiRunId(sbpRunId(commandLine))
                    .cleanup(booleanOptionWithDefault(commandLine, CLEANUP_FLAG, defaults.cleanup()))
                    .runId(runId(commandLine))
                    .cloudSdkPath(commandLine.getOptionValue(CLOUD_SDK_PATH_FLAG, defaults.cloudSdkPath()))
                    .usePreemptibleVms(booleanOptionWithDefault(commandLine, USE_PREEMTIBLE_VMS_FLAG, defaults.usePreemptibleVms()))
                    .useLocalSsds(booleanOptionWithDefault(commandLine, USE_LOCAL_SSDS_FLAG, defaults.useLocalSsds()))
                    .rclonePath(commandLine.getOptionValue(RCLONE_PATH_FLAG, defaults.rclonePath()))
                    .rcloneGcpRemote(commandLine.getOptionValue(RCLONE_GCP_REMOTE_FLAG, defaults.rcloneGcpRemote()))
                    .rcloneS3RemoteDownload(commandLine.getOptionValue(RCLONE_S3_REMOTE_DOWNLOAD_FLAG, defaults.rcloneS3RemoteDownload()))
                    .rcloneS3RemoteUpload(commandLine.getOptionValue(RCLONE_S3_REMOTE_UPLOAD_FLAG, defaults.rcloneS3RemoteUpload()))
                    .runBamMetrics(booleanOptionWithDefault(commandLine, RUN_METRICS_FLAG, defaults.runBamMetrics()))
                    .runSnpGenotyper(booleanOptionWithDefault(commandLine, RUN_SNP_GENOTYPER_FLAG, defaults.runSnpGenotyper()))
                    .runGermlineCaller(booleanOptionWithDefault(commandLine, RUN_GERMLINE_CALLER_FLAG, defaults.runGermlineCaller()))
                    .runSomaticCaller(booleanOptionWithDefault(commandLine, RUN_SOMATIC_CALLER_FLAG, defaults.runSomaticCaller()))
                    .runSageGermlineCaller(booleanOptionWithDefault(commandLine,
                            RUN_SAGE_GERMLINE_CALLER_FLAG,
                            defaults.runSageGermlineCaller()))
                    .runStructuralCaller(booleanOptionWithDefault(commandLine, RUN_STRUCTURAL_CALLER_FLAG, defaults.runStructuralCaller()))
                    .runTertiary(booleanOptionWithDefault(commandLine, RUN_TERTIARY_FLAG, defaults.runTertiary()))
                    .runHlaTyping(booleanOptionWithDefault(commandLine, RUN_HLA_TYPING_FLAG, defaults.runHlaTyping()))
                    .serviceAccountEmail(commandLine.getOptionValue(SERVICE_ACCOUNT_EMAIL_FLAG, defaults.serviceAccountEmail()))
                    .outputBucket(commandLine.getOptionValue(OUTPUT_BUCKET_FLAG, defaults.outputBucket()))
                    .archiveBucket(commandLine.getOptionValue(ARCHIVE_BUCKET_FLAG, defaults.archiveBucket()))
                    .archiveProject(commandLine.getOptionValue(ARCHIVE_PROJECT_FLAG, defaults.archiveProject()))
                    .archivePrivateKeyPath(commandLine.getOptionValue(ARCHIVE_PRIVATE_KEY_FLAG, defaults.archivePrivateKeyPath()))
                    .network(commandLine.getOptionValue(NETWORK_FLAG, defaults.network()))
                    .subnet(subnet(commandLine, defaults))
                    .tags(networkTags(commandLine, defaults))
                    .uploadPrivateKeyPath(commandLine.getOptionValue(UPLOAD_PRIVATE_KEY_FLAG, defaults.uploadPrivateKeyPath()))
                    .cmek(cmek(commandLine, defaults))
                    .shallow(booleanOptionWithDefault(commandLine, SHALLOW_FLAG, defaults.shallow()))
                    .outputCram(booleanOptionWithDefault(commandLine, OUTPUT_CRAM_FLAG, defaults.outputCram()))
                    .publishToTurquoise(booleanOptionWithDefault(commandLine, PUBLISH_TO_TURQUOISE_FLAG, defaults.publishToTurquoise()))
                    .pollInterval(Integer.parseInt(commandLine.getOptionValue(CommonArguments.POLL_INTERVAL,
                            defaults.pollInterval().toString())))
                    .zone(zone(commandLine, defaults))
                    .maxConcurrentLanes(maxConcurrentLanes(commandLine, defaults.maxConcurrentLanes()))
                    .profile(defaults.profile())
                    .uploadPrivateKeyPath(defaults.uploadPrivateKeyPath())
                    .refGenomeVersion(refGenomeVersion(commandLine, defaults))
                    .refGenomeUrl(refGenomeUrl(commandLine, defaults))
                    .sampleJson(sampleJson(commandLine, defaults))
                    .startingPoint(startingPoint(commandLine, defaults))
                    .imageName(imageName(commandLine, defaults))
                    .biopsy(biopsy(commandLine, defaults))
                    .imageProject(imageProject(commandLine, defaults))
                    .useCrams(booleanOptionWithDefault(commandLine, USE_CRAMS_FLAG, defaults.useCrams()))
                    .pubsubProject(pubsubProject(commandLine, defaults))
                    .build();
        } catch (ParseException e) {
            LOGGER.error("Could not parse command line args", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("pipeline5", options());
            throw e;
        }
    }

    public static Optional<String> pubsubProject(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(PUBSUB_PROJECT_FLAG)) {
            return Optional.of(commandLine.getOptionValue(PUBSUB_PROJECT_FLAG));
        }
        return defaults.pubsubProject();
    }

    public static Optional<String> biopsy(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(BIOPSY_FLAG)) {
            return Optional.of(commandLine.getOptionValue(BIOPSY_FLAG));
        }
        return defaults.biopsy();
    }

    public static Optional<String> subnet(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(SUBNET_FLAG)) {
            return Optional.of(commandLine.getOptionValue(SUBNET_FLAG));
        }
        return defaults.subnet();
    }

    public static List<String> networkTags(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(NETWORK_TAGS_FLAG)) {
            return List.of(commandLine.getOptionValue(NETWORK_TAGS_FLAG).split(","));
        }
        return defaults.tags();
    }

    private static Optional<String> refGenomeUrl(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(REF_GENOME_URL)) {
            return Optional.of(commandLine.getOptionValue(REF_GENOME_URL));
        }
        return defaults.refGenomeUrl();
    }

    private static Optional<String> startingPoint(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(STARTING_POINT_FLAG)) {
            return Optional.of(commandLine.getOptionValue(STARTING_POINT_FLAG));
        }
        return defaults.startingPoint();
    }

    private static Optional<String> cmek(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(CMEK_FLAG)) {
            return Optional.of(commandLine.getOptionValue(CMEK_FLAG));
        }
        return defaults.cmek();
    }

    private static Optional<String> sampleJson(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(SAMPLE_JSON_FLAG)) {
            return Optional.of(commandLine.getOptionValue(SAMPLE_JSON_FLAG));
        }
        return defaults.sampleJson();
    }

    private static Optional<String> imageProject(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(IMAGE_PROJECT_FLAG)) {
            return Optional.of(commandLine.getOptionValue(IMAGE_PROJECT_FLAG));
        }
        return defaults.imageProject();
    }

    private static Optional<String> imageName(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(IMAGE_NAME_FLAG)) {
            return Optional.of(commandLine.getOptionValue(IMAGE_NAME_FLAG));
        }
        return defaults.imageName();
    }

    private static Optional<String> zone(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(ZONE_FLAG)) {
            return Optional.of(commandLine.getOptionValue(ZONE_FLAG));
        }
        return defaults.zone();
    }

    private static RefGenomeVersion refGenomeVersion(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(REF_GENOME_VERSION_FLAG)) {
            return Arrays.stream(RefGenomeVersion.values())
                    .filter(version -> version.pipeline().equals(commandLine.getOptionValue(REF_GENOME_VERSION_FLAG)))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(refGenomeVersions().collect(Collectors.joining(","))));
        }
        return defaults.refGenomeVersion();
    }

    private static Stream<String> refGenomeVersions() {
        return Arrays.stream(RefGenomeVersion.values()).map(RefGenomeVersion::pipeline);
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

    private static int maxConcurrentLanes(final CommandLine commandLine, final int defaultValue) {
        try {
            if (commandLine.hasOption(MAX_CONCURRENT_LANES_FLAG)) {
                return Integer.parseInt(commandLine.getOptionValue(MAX_CONCURRENT_LANES_FLAG));
            }
            return defaultValue;
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean booleanOptionWithDefault(final CommandLine commandLine, final String flag, final boolean defaultValue)
            throws ParseException {
        String value = commandLine.getOptionValue(flag, Boolean.toString(defaultValue));
        if (!value.equals("true") && !value.equals("false")) {
            throw new ParseException(flag + " is a flag and only accepts true|false");
        }
        return Boolean.parseBoolean(value);
    }

    private static Optional<String> runId(CommandLine commandLine) {
        if (commandLine.hasOption(RUN_ID_FLAG)) {
            return Optional.of(commandLine.getOptionValue(RUN_ID_FLAG));
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
