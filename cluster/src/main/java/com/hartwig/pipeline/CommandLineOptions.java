package com.hartwig.pipeline;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.pipeline.Arguments.DefaultsProfile;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.tools.VersionUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandLineOptions {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineOptions.class);
    private static final String CLEANUP_FLAG = "cleanup";
    private static final String PROJECT_FLAG = "project";
    private static final String REGION_FLAG = "region";
    private static final String RUN_TAG_FLAG = "run_tag";
    private static final String CLOUD_SDK_PATH_FLAG = "cloud_sdk";
    private static final String USE_PREEMTIBLE_VMS_FLAG = "preemptible_vms";
    private static final String USE_LOCAL_SSDS_FLAG = "local_ssds";
    private static final String VM_SELF_DELETE_ON_SHUTDOWN_FLAG = "vm_self_delete_on_shutdown";
    private static final String LOG_DEBUG_FLAG = "log_debug";
    private static final String VERBOSE_CLOUD_SDK_FLAG = "verbose_cloud_sdk";
    private static final String RUN_METRICS_FLAG = "run_bam_metrics";
    private static final String PROFILE_FLAG = "profile";
    private static final String SERVICE_ACCOUNT_EMAIL_FLAG = "service_account_email";
    private static final String MAX_CONCURRENT_LANES_FLAG = "max_concurrent_lanes";
    private static final String DEFAULT_PROFILE = "production";
    private static final String OUTPUT_CRAM_FLAG = "output_cram";
    private static final String PUBLISH_TO_TURQUOISE_FLAG = "publish_to_turquoise";
    private static final String RUN_GERMLINE_CALLER_FLAG = "run_germline_caller";
    private static final String RUN_SNP_GENOTYPER_FLAG = "run_snp_genotyper";
    private static final String RUN_TERTIARY_FLAG = "run_tertiary";
    private static final String OUTPUT_BUCKET_FLAG = "output_bucket";
    private static final String SET_NAME_FLAG = "set_id";
    private static final String NETWORK_FLAG = "network";
    private static final String SUBNET_FLAG = "subnet";
    private static final String NETWORK_TAGS_FLAG = "network_tags";
    private static final String CMEK_FLAG = "cmek";
    private static final String SHALLOW_FLAG = "shallow";
    private static final String MACHINE_FAMILIES = "machine_families";
    private static final String REF_GENOME_VERSION_FLAG = "ref_genome_version";
    private static final String SAMPLE_JSON_FLAG = "sample_json";
    private static final String STARTING_POINT_FLAG = "starting_point";
    private static final String IMAGE_NAME_FLAG = "image_name";
    private static final String BIOPSY_FLAG = "biopsy";
    private static final String HMF_API_URL_FLAG = "hmf_api_url";
    private static final String IMAGE_PROJECT_FLAG = "image_project";
    private static final String USE_CRAMS_FLAG = "use_crams";
    private static final String PUBSUB_PROJECT_FLAG = "pubsub_project";
    private static final String ANONYMIZE_FLAG = "anonymize";
    private static final String CONTEXT_FLAG = "context";
    private static final String COST_CENTER_LABEL_FLAG = "cost_center_label";
    private static final String USER_LABEL_FLAG = "user_label";
    private static final String USE_TARGET_REGIONS = "use_target_regions";
    private static final String PUBLISH_DB_LOAD_EVENT_FLAG = "publish_db_load_event";
    private static final String PUBSUB_WORKFLOW_FLAG = "pubsub_workflow";
    private static final String PUBSUB_ENVIRONMENT_FLAG = "pubsub_environment";
    private static final String PUBLISH_EVENTS_ONLY_FLAG = "publish_events_only";
    private static final String USE_PRIVATE_RESOURCES_FLAG = "use_private_resources";
    private static final String REDO_DUPLICATE_MARKING_FLAG = "redo_duplicate_marking";
    private static final String STAGE_MEMORY_OVERRIDE_GB_FLAG = "stage_memory_override_gb";
    private static final String STAGE_MEMORY_OVERRIDE_REGEX_FLAG = "stage_memory_override_regex";

    private static Options options() {
        return new Options().addOption(profile())
                .addOption(setId())
                .addOption(optionWithBooleanArg(CLEANUP_FLAG, "Don't delete the runtime bucket after job is complete"))
                .addOption(optionWithBooleanArg(USE_PREEMTIBLE_VMS_FLAG,
                        "Use preemtible VMs to lower cost. "
                                + "These VMs can be reclaimed at any time so using them can make the run less stable."))
                .addOption(optionWithBooleanArg(USE_LOCAL_SSDS_FLAG,
                        "Use local (ephemeral) SSDs rather than persistent storage to lower cost and improve performance. "
                                + "VMs started with these devices can only be started once so may not be suitable for development"))
                .addOption(Option.builder(LOG_DEBUG_FLAG).desc("Turn on debug logging").build())
                .addOption(optionWithBooleanArg(VERBOSE_CLOUD_SDK_FLAG,
                        "Have stdout and stderr of Google tools like gsutil stream to the console"))
                .addOption(project())
                .addOption(region())
                .addOption(runTag())
                .addOption(gsutilPath())
                .addOption(optionWithBooleanArg(RUN_METRICS_FLAG, "Run wgs metricsOutputFile after BAM creation"))
                .addOption(optionWithBooleanArg(RUN_GERMLINE_CALLER_FLAG, "Run germline calling (gatk) on a VM"))
                .addOption(optionWithBooleanArg(RUN_TERTIARY_FLAG, "Run tertiary analysis algorithms (amber, cobalt, purple, cuppa, etc)"))
                .addOption(optionWithBooleanArg(RUN_SNP_GENOTYPER_FLAG, "Run snp genotyper for QC against genotyping"))
                .addOption(serviceAccountEmail())
                .addOption(patientReportBucket())
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
                .addOption(machineFamilies())
                .addOption(refGenomeVersion())
                .addOption(maxConcurrentLanes())
                .addOption(json())
                .addOption(startingPoint())
                .addOption(imageName())
                .addOption(biopsy())
                .addOption(hmfApiUrl())
                .addOption(imageProject())
                .addOption(useCrams())
                .addOption(pubsubProject())
                .addOption(anonymize())
                .addOption(usePrivateResources())
                .addOption(context())
                .addOption(costCenterLabel())
                .addOption(userLabel())
                .addOption(useTargetRegions())
                .addOption(publishDbLoadEvent())
                .addOption(pubsubTopicWorkflow())
                .addOption(pubsubTopicEnvironment())
                .addOption(optionWithBooleanArg(VM_SELF_DELETE_ON_SHUTDOWN_FLAG, "Delete VMs on shutdown"))
                .addOption(optionWithBooleanArg(PUBLISH_EVENTS_ONLY_FLAG,
                        "Compute nothing, only publish events for downstream consumption"))
                .addOption(optionWithBooleanArg(REDO_DUPLICATE_MARKING_FLAG, "Redo duplicate marking on input BAM or CRAM"));
    }

    private static Option useTargetRegions() {
        return optionWithBooleanArg(USE_TARGET_REGIONS, "Enable target-regions mode");
    }

    private static Option userLabel() {
        return optionWithArg(USER_LABEL_FLAG,
                "This value will be applied as a label with key cost_center to all resources (VMs, disks and buckets)");
    }

    private static Option costCenterLabel() {
        return optionWithArg(COST_CENTER_LABEL_FLAG,
                "This value will be applied as a label with key cost_center to all resources (VMs, disks and buckets)");
    }

    private static Option context() {
        return optionWithArg(CONTEXT_FLAG,
                format("Context in which this pipeline is run [%s]. Impacts downstream handling of results in production environment",
                        Stream.of(Pipeline.Context.values()).map(Pipeline.Context::name).collect(Collectors.joining(", "))));
    }

    private static Option anonymize() {
        return optionWithArg(ANONYMIZE_FLAG, "Use barcodes instead of sample names in files, headers and annotations");
    }

    private static Option usePrivateResources() {
        return optionWithArg(USE_PRIVATE_RESOURCES_FLAG, "Use resources specific to private repo where applicable");
    }

    private static Option pubsubProject() {
        return optionWithArg(PUBSUB_PROJECT_FLAG, "Project to publish pipeline events over pub/sub");
    }

    private static Option pubsubTopicWorkflow() {
        return optionWithArg(PUBSUB_WORKFLOW_FLAG, "Workflow to use in the pub/sub event context (ie analysis, verification)");
    }

    private static Option pubsubTopicEnvironment() {
        return optionWithArg(PUBSUB_ENVIRONMENT_FLAG, "Environment to use in the pub/sub event context (ie prod-1, protect)");
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

    private static Option hmfApiUrl() {
        return optionWithArg(HMF_API_URL_FLAG, "URL of the HMF-API.");
    }

    private static Option startingPoint() {
        return optionWithArg(STARTING_POINT_FLAG,
                "Run from a starting point after fastq alignment. Specify \"help\" for this argument to see starting points.");
    }

    private static Option json() {
        return optionWithArg(SAMPLE_JSON_FLAG, "JSON file defining the location of FASTQ inputs in GCP.");
    }

    private static Option imageName() {
        return optionWithArg(IMAGE_NAME_FLAG, String.format("Image to use instead of the latest %s image", VersionUtils.imageVersion()));
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

    private static Option setId() {
        return optionWithArg(SET_NAME_FLAG,
                "The id of the set for which to run a somatic pipeline. A set represents a valid reference/tumor pair (ie CPCT12345678). "
                        + "Can only be used when mode is somatic and the single sample pipelines have been run for each sample");
    }

    private static Option patientReportBucket() {
        return optionWithArg(OUTPUT_BUCKET_FLAG, "Bucket in which to persist the final patient report and accompanying data.");
    }

    private static Option profile() {
        return optionWithArg(PROFILE_FLAG,
                format("Defaults profile to use. Accepts [%s]",
                        Arrays.stream(DefaultsProfile.values()).map(Enum::name).collect(Collectors.joining("|"))));
    }

    private static Option gsutilPath() {
        return optionWithArg(CLOUD_SDK_PATH_FLAG, "Path to the google cloud sdk bin directory (with gsutil and gcloud)");
    }

    private static Option runTag() {
        return optionWithArg(RUN_TAG_FLAG, "Override the generated run id used for runtime bucket and cluster naming");
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

    private static Option machineFamilies() {
        return optionWithArg(MACHINE_FAMILIES, "Machine families overrides");
    }

    private static Option refGenomeVersion() {
        return optionWithArg(REF_GENOME_VERSION_FLAG,
                format("Ref genome version, default [%s], values [%s]",
                        RefGenomeVersion.V37.numeric(),
                        refGenomeVersions().collect(Collectors.joining(","))));
    }

    private static Option publishDbLoadEvent() {
        return optionWithBooleanArg(PUBLISH_DB_LOAD_EVENT_FLAG,
                format("Publish an event for downstream DB load; has no effect unless context is [%s]", Pipeline.Context.PLATINUM));
    }

    public static Arguments from(final String[] args) throws ParseException {
        try {
            DefaultParser defaultParser = new DefaultParser();
            CommandLine commandLine = defaultParser.parse(options(), args);
            Arguments defaults = Arguments.defaults(commandLine.getOptionValue(PROFILE_FLAG, DEFAULT_PROFILE));

            return Arguments.builder()
                    .setName(commandLine.getOptionValue(SET_NAME_FLAG, defaults.setName()))
                    .project(commandLine.getOptionValue(PROJECT_FLAG, defaults.project()))
                    .region(handleDashesInRegion(commandLine, defaults.region()))
                    .cleanup(booleanOptionWithDefault(commandLine, CLEANUP_FLAG, defaults.cleanup()))
                    .runTag(runTag(commandLine))
                    .cloudSdkPath(commandLine.getOptionValue(CLOUD_SDK_PATH_FLAG, defaults.cloudSdkPath()))
                    .usePreemptibleVms(booleanOptionWithDefault(commandLine, USE_PREEMTIBLE_VMS_FLAG, defaults.usePreemptibleVms()))
                    .useLocalSsds(booleanOptionWithDefault(commandLine, USE_LOCAL_SSDS_FLAG, defaults.useLocalSsds()))
                    .vmSelfDeleteOnShutdown(booleanOptionWithDefault(commandLine, VM_SELF_DELETE_ON_SHUTDOWN_FLAG, defaults.vmSelfDeleteOnShutdown()))
                    .logDebug(commandLine.hasOption(LOG_DEBUG_FLAG))
                    .redoDuplicateMarking(booleanOptionWithDefault(commandLine, REDO_DUPLICATE_MARKING_FLAG, defaults.redoDuplicateMarking()))
                    .runBamMetrics(booleanOptionWithDefault(commandLine, RUN_METRICS_FLAG, defaults.runBamMetrics()))
                    .runSnpGenotyper(booleanOptionWithDefault(commandLine, RUN_SNP_GENOTYPER_FLAG, defaults.runSnpGenotyper()))
                    .runGermlineCaller(booleanOptionWithDefault(commandLine, RUN_GERMLINE_CALLER_FLAG, defaults.runGermlineCaller()))
                    .runTertiary(booleanOptionWithDefault(commandLine, RUN_TERTIARY_FLAG, defaults.runTertiary()))
                    .serviceAccountEmail(commandLine.getOptionValue(SERVICE_ACCOUNT_EMAIL_FLAG, defaults.serviceAccountEmail()))
                    .outputBucket(commandLine.getOptionValue(OUTPUT_BUCKET_FLAG, defaults.outputBucket()))
                    .network(commandLine.getOptionValue(NETWORK_FLAG, defaults.network()))
                    .subnet(subnet(commandLine, defaults))
                    .tags(networkTags(commandLine, defaults))
                    .cmek(cmek(commandLine, defaults))
                    .shallow(booleanOptionWithDefault(commandLine, SHALLOW_FLAG, defaults.shallow()))
                    .outputCram(booleanOptionWithDefault(commandLine, OUTPUT_CRAM_FLAG, defaults.outputCram()))
                    .publishToTurquoise(booleanOptionWithDefault(commandLine, PUBLISH_TO_TURQUOISE_FLAG, defaults.publishToTurquoise()))
                    .pollInterval(Integer.parseInt(commandLine.getOptionValue(CommonArguments.POLL_INTERVAL,
                            defaults.pollInterval().toString())))
                    .machineFamilies(machineFamily(commandLine))
                    .maxConcurrentLanes(maxConcurrentLanes(commandLine, defaults.maxConcurrentLanes()))
                    .profile(defaults.profile())
                    .refGenomeVersion(refGenomeVersion(commandLine, defaults))
                    .sampleJson(commandLine.getOptionValue(SAMPLE_JSON_FLAG, defaults.sampleJson()))
                    .startingPoint(startingPoint(commandLine, defaults))
                    .imageName(imageName(commandLine, defaults))
                    .biopsy(biopsy(commandLine, defaults))
                    .imageProject(imageProject(commandLine, defaults))
                    .useCrams(booleanOptionWithDefault(commandLine, USE_CRAMS_FLAG, defaults.useCrams()))
                    .pubsubProject(pubsubProject(commandLine, defaults))
                    .anonymize(booleanOptionWithDefault(commandLine, ANONYMIZE_FLAG, defaults.anonymize()))
                    .usePrivateResources(booleanOptionWithDefault(commandLine, USE_PRIVATE_RESOURCES_FLAG, defaults.publishEventsOnly()))
                    .context(context(commandLine, defaults))
                    .costCenterLabel(costCenterLabel(commandLine, defaults))
                    .userLabel(userLabel(commandLine, defaults))
                    .useTargetRegions(booleanOptionWithDefault(commandLine, USE_TARGET_REGIONS, defaults.useTargetRegions()))
                    .publishDbLoadEvent(booleanOptionWithDefault(commandLine, PUBLISH_DB_LOAD_EVENT_FLAG, defaults.publishDbLoadEvent()))
                    .pubsubTopicWorkflow(pubsubTopicWorkflow(commandLine, defaults))
                    .pubsubTopicEnvironment(pubsubTopicEnvironment(commandLine, defaults))
                    .publishEventsOnly(booleanOptionWithDefault(commandLine, PUBLISH_EVENTS_ONLY_FLAG, defaults.publishEventsOnly()))
                    .hmfApiUrl(hmfApiUrl(commandLine, defaults))
                    .stageMemoryOverrideRegex(stageMemoryOverrideRegex(commandLine))
                    .stageMemoryOverrideGb(stageMemoryOverrideGb(commandLine))
                    .build();
        } catch (ParseException e) {
            LOGGER.error("Could not parse command line args", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("pipeline5", options());
            throw e;
        }
    }

    public static Optional<String> userLabel(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(USER_LABEL_FLAG)) {
            return Optional.of(commandLine.getOptionValue(USER_LABEL_FLAG));
        }
        return defaults.costCenterLabel();
    }

    public static Optional<String> costCenterLabel(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(COST_CENTER_LABEL_FLAG)) {
            return Optional.of(commandLine.getOptionValue(COST_CENTER_LABEL_FLAG));
        }
        return defaults.costCenterLabel();
    }

    public static Pipeline.Context context(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(CONTEXT_FLAG)) {
            return Pipeline.Context.valueOf(commandLine.getOptionValue(CONTEXT_FLAG));
        }
        return defaults.context();
    }

    public static Optional<String> pubsubProject(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(PUBSUB_PROJECT_FLAG)) {
            return Optional.of(commandLine.getOptionValue(PUBSUB_PROJECT_FLAG));
        }
        return defaults.pubsubProject();
    }

    public static Optional<String> pubsubTopicWorkflow(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(PUBSUB_WORKFLOW_FLAG)) {
            return Optional.of(commandLine.getOptionValue(PUBSUB_WORKFLOW_FLAG));
        }
        return defaults.pubsubTopicWorkflow();
    }

    public static Optional<String> pubsubTopicEnvironment(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(PUBSUB_ENVIRONMENT_FLAG)) {
            return Optional.of(commandLine.getOptionValue(PUBSUB_ENVIRONMENT_FLAG));
        }
        return defaults.pubsubTopicWorkflow();
    }

    public static Optional<String> biopsy(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(BIOPSY_FLAG)) {
            return Optional.of(commandLine.getOptionValue(BIOPSY_FLAG));
        }
        return defaults.biopsy();
    }

    public static Optional<String> hmfApiUrl(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(HMF_API_URL_FLAG)) {
            return Optional.of(commandLine.getOptionValue(HMF_API_URL_FLAG));
        }
        return defaults.hmfApiUrl();
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

    private static List<String> machineFamily(final CommandLine commandLine) {
        if (commandLine.hasOption(MACHINE_FAMILIES)) {
            return List.of(commandLine.getOptionValue(MACHINE_FAMILIES).split(","));
        }
        return List.of();
    }

    private static RefGenomeVersion refGenomeVersion(final CommandLine commandLine, final Arguments defaults) {
        if (commandLine.hasOption(REF_GENOME_VERSION_FLAG)) {
            return Arrays.stream(RefGenomeVersion.values())
                    .filter(version -> version.numeric().equals(commandLine.getOptionValue(REF_GENOME_VERSION_FLAG)))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(refGenomeVersions().collect(Collectors.joining(","))));
        }
        return defaults.refGenomeVersion();
    }

    private static Stream<String> refGenomeVersions() {
        return Arrays.stream(RefGenomeVersion.values()).map(RefGenomeVersion::numeric);
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

    private static Optional<String> runTag(final CommandLine commandLine) {
        if (commandLine.hasOption(RUN_TAG_FLAG)) {
            return Optional.of(commandLine.getOptionValue(RUN_TAG_FLAG));
        }
        return Optional.empty();
    }

    private static String handleDashesInRegion(final CommandLine commandLine, final String defaultRegion) {
        if (commandLine.hasOption(REGION_FLAG)) {
            return commandLine.getOptionValue(REGION_FLAG);
        }
        return defaultRegion;
    }

    private static Optional<String> stageMemoryOverrideRegex(final CommandLine commandLine) {
        if (commandLine.hasOption(STAGE_MEMORY_OVERRIDE_REGEX_FLAG)) {
            return Optional.of(commandLine.getOptionValue(STAGE_MEMORY_OVERRIDE_REGEX_FLAG));
        }
        return Optional.empty();
    }

    private static Optional<Integer> stageMemoryOverrideGb(final CommandLine commandLine) {
        if (commandLine.hasOption(STAGE_MEMORY_OVERRIDE_GB_FLAG)) {
            return Optional.of(Integer.parseInt(commandLine.getOptionValue(STAGE_MEMORY_OVERRIDE_GB_FLAG)));
        }
        return Optional.empty();
    }

    private static Option optionWithArg(final String option, final String description) {
        return Option.builder(option).hasArg().argName(option).desc(description).build();
    }

    private static Option optionWithBooleanArg(final String option, final String description) {
        return Option.builder(option).hasArg().argName("true|false").desc(description).build();
    }
}
