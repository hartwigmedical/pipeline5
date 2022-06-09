package com.hartwig.pipeline.tertiary.rose;

import static com.hartwig.pipeline.execution.vm.InputDownload.initialiseOptionalLocation;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutput;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutput;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutputLocations;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tertiary.virus.VirusOutput;
import com.hartwig.pipeline.tools.Versions;

@Namespace(Rose.NAMESPACE)
public class Rose implements Stage<RoseOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "rose";
    private final ResourceFiles resourceFiles;
    private final InputDownload purplePurity;
    private final InputDownload purpleQc;
    private final InputDownload purpleGeneCopyNumber;
    private final InputDownload purpleSomaticDriverCatalog;
    private final InputDownload purpleGermlineDriverCatalog;
    private final InputDownload purpleSomaticVariants;
    private final InputDownload purpleGermlineVariants;
    private final InputDownload linxFusions;
    private final InputDownload linxBreakends;
    private final InputDownload linxDriverCatalog;
    private final InputDownload virusAnnotations;
    private final InputDownload chordPredictions;
    private final InputDownload molecularTissueOrigins;

    public Rose(final ResourceFiles resourceFiles, final PurpleOutput purpleOutput, final LinxSomaticOutput linxSomaticOutput,
            final VirusOutput virusOutput, final ChordOutput chordOutput, final CuppaOutput cuppaOutput) {
        this.resourceFiles = resourceFiles;
        PurpleOutputLocations purple = purpleOutput.outputLocations();
        this.purplePurity = new InputDownload(purple.purity());
        this.purpleQc = new InputDownload(purple.qcFile());
        this.purpleGeneCopyNumber = initialiseOptionalLocation(purple.geneCopyNumber());
        this.purpleSomaticDriverCatalog = initialiseOptionalLocation(purple.somaticDriverCatalog());
        this.purpleGermlineDriverCatalog = initialiseOptionalLocation(purple.germlineDriverCatalog());
        this.purpleSomaticVariants = initialiseOptionalLocation(purple.somaticVariants());
        this.purpleGermlineVariants = initialiseOptionalLocation(purple.germlineVariants());
        LinxSomaticOutputLocations linx = linxSomaticOutput.linxOutputLocations();
        this.linxFusions = new InputDownload(linx.fusions());
        this.linxBreakends = new InputDownload(linx.breakends());
        this.linxDriverCatalog = new InputDownload(linx.driverCatalog());
        this.virusAnnotations = new InputDownload(virusOutput.virusAnnotations());
        this.chordPredictions = new InputDownload(chordOutput.predictions());
        this.molecularTissueOrigins = new InputDownload(cuppaOutput.cuppaOutputLocations().conclusionTxt());
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purplePurity,
                purpleQc,
                purpleGeneCopyNumber,
                purpleSomaticDriverCatalog,
                purpleGermlineDriverCatalog,
                purpleSomaticVariants,
                purpleGermlineVariants,
                linxFusions,
                linxBreakends,
                linxDriverCatalog,
                virusAnnotations,
                chordPredictions,
                molecularTissueOrigins);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .name(NAMESPACE)
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(2, 10))
                .workingDiskSpaceGb(375)
                .build();
    }

    @Override
    public RoseOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return RoseOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .build();
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        List<String> arguments = List.of("-actionability_database_tsv",
                resourceFiles.roseActionabilityDb(),
                "-ref_genome_version",
                resourceFiles.version().rose(),
                "-driver_gene_37_tsv",
                resourceFiles.driverGenePanel(),
                "-driver_gene_38_tsv",
                resourceFiles.driverGenePanel(),
                "-purple_purity_tsv",
                purplePurity.getLocalTargetPath(),
                "-purple_qc_file",
                purpleQc.getLocalTargetPath(),
                "-purple_gene_copy_number_tsv",
                purpleGeneCopyNumber.getLocalTargetPath(),
                "-purple_somatic_driver_catalog_tsv",
                purpleSomaticDriverCatalog.getLocalTargetPath(),
                "-purple_germline_driver_catalog_tsv",
                purpleGermlineDriverCatalog.getLocalTargetPath(),
                "-purple_somatic_variant_vcf",
                purpleSomaticVariants.getLocalTargetPath(),
                "-purple_germline_variant_vcf",
                purpleGermlineVariants.getLocalTargetPath(),
                "-linx_fusion_tsv",
                linxFusions.getLocalTargetPath(),
                "-linx_breakend_tsv",
                linxBreakends.getLocalTargetPath(),
                "-linx_driver_catalog_tsv",
                linxDriverCatalog.getLocalTargetPath(),
                "-annotated_virus_tsv",
                virusAnnotations.getLocalTargetPath(),
                "-chord_prediction_txt",
                chordPredictions.getLocalTargetPath(),
                "-molecular_tissue_origin_txt",
                molecularTissueOrigins.getLocalTargetPath(),
                "-output_dir",
                VmDirectories.OUTPUT,
                "-tumor_sample_id",
                metadata.tumor().sampleName(),
                "-ref_sample_id",
                metadata.reference().sampleName());
        return List.of(new JavaJarCommand("rose", Versions.ROSE, "rose.jar", "8G", arguments));
    }

    @Override
    public RoseOutput skippedOutput(final SomaticRunMetadata metadata) {
        return RoseOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary();
    }
}
