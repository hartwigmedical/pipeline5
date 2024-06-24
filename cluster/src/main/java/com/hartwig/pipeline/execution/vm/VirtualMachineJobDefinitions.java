package com.hartwig.pipeline.execution.vm;

import static com.hartwig.computeengine.execution.vm.VirtualMachinePerformanceProfile.custom;
import static com.hartwig.pipeline.tools.HmfTool.AMBER;
import static com.hartwig.pipeline.tools.HmfTool.BAM_TOOLS;
import static com.hartwig.pipeline.tools.HmfTool.CHORD;
import static com.hartwig.pipeline.tools.HmfTool.CIDER;
import static com.hartwig.pipeline.tools.HmfTool.COBALT;
import static com.hartwig.pipeline.tools.HmfTool.CUPPA;
import static com.hartwig.pipeline.tools.HmfTool.GRIDSS;
import static com.hartwig.pipeline.tools.HmfTool.GRIPSS;
import static com.hartwig.pipeline.tools.HmfTool.HEALTH_CHECKER;
import static com.hartwig.pipeline.tools.HmfTool.LILAC;
import static com.hartwig.pipeline.tools.HmfTool.LINX;
import static com.hartwig.pipeline.tools.HmfTool.MARK_DUPS;
import static com.hartwig.pipeline.tools.HmfTool.PAVE;
import static com.hartwig.pipeline.tools.HmfTool.PEACH;
import static com.hartwig.pipeline.tools.HmfTool.PURPLE;
import static com.hartwig.pipeline.tools.HmfTool.SAGE;
import static com.hartwig.pipeline.tools.HmfTool.SIGS;
import static com.hartwig.pipeline.tools.HmfTool.TEAL;
import static com.hartwig.pipeline.tools.HmfTool.VIRUS_INTERPRETER;

import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.calling.sage.SageConfiguration;
import com.hartwig.pipeline.calling.structural.gridss.Gridss;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.flagstat.Flagstat;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.chord.Chord;
import com.hartwig.pipeline.tertiary.cider.Cider;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cuppa.Cuppa;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.lilac.Lilac;
import com.hartwig.pipeline.tertiary.lilac.LilacBamSlicer;
import com.hartwig.pipeline.tertiary.peach.Peach;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.sigs.Sigs;
import com.hartwig.pipeline.tertiary.teal.Teal;
import com.hartwig.pipeline.tertiary.virus.VirusBreakend;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreter;
import com.hartwig.pipeline.tools.VersionUtils;

public final class VirtualMachineJobDefinitions {

    private VirtualMachineJobDefinitions() {

    }

    private static final String STANDARD_IMAGE = "pipeline5-" + VersionUtils.imageVersion();
    private static final int MINIMAL_DISK_SPACE_GB = 0;

    public static VirtualMachineJobDefinition snpGenotyping(final BashStartupScript startupScript,
            final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name("snpgenotype")
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(4, 16))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition germlineCalling(final BashStartupScript startupScript,
            final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name("germline")
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(32, 40))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition sageSomaticCalling(final BashStartupScript startupScript,
            final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(SageConfiguration.SAGE_SOMATIC_NAMESPACE.replace("_", "-"))
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(SAGE.getCpus(), SAGE.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition sageGermlineCalling(final BashStartupScript startupScript,
            final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(SageConfiguration.SAGE_GERMLINE_NAMESPACE.replace("_", "-"))
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(SAGE.getCpus(), SAGE.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition gridds(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Gridss.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(GRIDSS.getCpus(), GRIDSS.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition gripss(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory,
            final String namespace) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(namespace.replace("_", "-"))
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(GRIPSS.getCpus(), GRIPSS.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition purple(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Purple.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(PURPLE.getCpus(), PURPLE.getMemoryGb()))
                .startupCommand(bash)
                .workingDiskSpaceGb(MINIMAL_DISK_SPACE_GB)
                .build();
    }

    public static VirtualMachineJobDefinition pave(final BashStartupScript bash, final ResultsDirectory resultsDirectory,
            final String namespace) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(namespace.replace("_", "-"))
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(PAVE.getCpus(), PAVE.getMemoryGb()))
                .startupCommand(bash)
                .workingDiskSpaceGb(MINIMAL_DISK_SPACE_GB)
                .build();
    }

    public static VirtualMachineJobDefinition amber(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Amber.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(AMBER.getCpus(), AMBER.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition virusbreakend(final BashStartupScript startupScript,
            final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(VirusBreakend.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(GRIDSS.getCpus(), GRIDSS.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition cobalt(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Cobalt.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(COBALT.getCpus(), COBALT.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition bamMetrics(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(BamMetrics.NAMESPACE.replace("_", "-"))
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(BAM_TOOLS.getCpus(), BAM_TOOLS.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition healthChecker(final BashStartupScript startupScript,
            final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(HealthChecker.NAMESPACE.replace("_", "-"))
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(HEALTH_CHECKER.getCpus(), HEALTH_CHECKER.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition flagstat(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Flagstat.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(32, 48))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition alignment(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory,
            final String name) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(name)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(64, 128))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition mergeMarkdups(final BashStartupScript startupScript,
            final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name("merge-markdup")
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(MARK_DUPS.getCpus(), MARK_DUPS.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition linx(final String type, final BashStartupScript startupScript,
            final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name("linx-" + type)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(LINX.getCpus(), LINX.getMemoryGb()))
                .startupCommand(startupScript)
                .workingDiskSpaceGb(MINIMAL_DISK_SPACE_GB)
                .build();
    }

    public static VirtualMachineJobDefinition chord(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Chord.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(CHORD.getCpus(), CHORD.getMemoryGb()))
                .startupCommand(startupScript)
                .workingDiskSpaceGb(MINIMAL_DISK_SPACE_GB)
                .build();
    }

    public static VirtualMachineJobDefinition cider(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Cider.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(CIDER.getCpus(), CIDER.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition cram2Bam(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory,
            final SingleSampleRunMetadata.SampleType sampleType, final String namespace) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(namespace)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(32, 32))
                .startupCommand(startupScript)
                .workingDiskSpaceGb(sampleType.equals(SingleSampleRunMetadata.SampleType.REFERENCE) ? 650 : 950)
                .build();
    }

    public static VirtualMachineJobDefinition cramConversion(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory,
            final SingleSampleRunMetadata.SampleType sampleType) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(CramConversion.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(CramConversion.NUMBER_OF_CORES, 6))
                .startupCommand(startupScript)
                .workingDiskSpaceGb(sampleType.equals(SingleSampleRunMetadata.SampleType.REFERENCE) ? 650 : 950)
                .build();
    }

    public static VirtualMachineJobDefinition cuppa(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Cuppa.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(CUPPA.getCpus(), CUPPA.getMemoryGb()))
                .startupCommand(startupScript)
                .workingDiskSpaceGb(MINIMAL_DISK_SPACE_GB)
                .build();
    }

    public static VirtualMachineJobDefinition lilac(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Lilac.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(LILAC.getCpus(), LILAC.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition lilacBamSlicer(final BashStartupScript startupScript,
            final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(LilacBamSlicer.NAMESPACE.replaceAll("_", "-"))
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(8, 16))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition orange(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory,
            final String namespace) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(namespace.replace("_", "-"))
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 18))
                .startupCommand(startupScript)
                .workingDiskSpaceGb(MINIMAL_DISK_SPACE_GB)
                .build();
    }

    public static VirtualMachineJobDefinition peach(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Peach.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(PEACH.getCpus(), PEACH.getMemoryGb()))
                .startupCommand(startupScript)
                .workingDiskSpaceGb(MINIMAL_DISK_SPACE_GB)
                .build();
    }

    public static VirtualMachineJobDefinition sigs(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Sigs.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(SIGS.getCpus(), SIGS.getMemoryGb()))
                .startupCommand(startupScript)
                .workingDiskSpaceGb(MINIMAL_DISK_SPACE_GB)
                .build();
    }

    public static VirtualMachineJobDefinition teal(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(Teal.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(TEAL.getCpus(), TEAL.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }

    public static VirtualMachineJobDefinition virusInterperter(final BashStartupScript startupScript,
            final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(STANDARD_IMAGE)
                .name(VirusInterpreter.NAMESPACE)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(VIRUS_INTERPRETER.getCpus(), VIRUS_INTERPRETER.getMemoryGb()))
                .startupCommand(startupScript)
                .build();
    }
}