package com.hartwig.pipeline.tertiary.virus;

import java.io.File;
import java.util.List;

import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.tools.Versions;

public class VirusInterpreter extends SubStage {

    private final String sampleId;
    private final ResourceFiles resourceFiles;
    private final String purplePurityTsvPath;
    private final String purpleQcFilePath;
    private final String tumorBamMetricsPath;

    public VirusInterpreter(final String sampleId, final ResourceFiles resourceFiles, final String purplePurityTsvPath,
            final String purpleQcFilePath, final String tumorBamMetricsPath) {
        super("virus.annotated", FileTypes.TSV);
        this.sampleId = sampleId;
        this.resourceFiles = resourceFiles;
        this.purplePurityTsvPath = purplePurityTsvPath;
        this.purpleQcFilePath = purpleQcFilePath;
        this.tumorBamMetricsPath = tumorBamMetricsPath;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return List.of(new JavaJarCommand("virus-interpreter",
                Versions.VIRUS_INTERPRETER,
                "virus-interpreter.jar",
                "2G",
                List.of("-sample_id",
                        sampleId,
                        "-purple_purity_tsv",
                        purplePurityTsvPath,
                        "-purple_qc_file",
                        purpleQcFilePath,
                        "-tumor_sample_wgs_metrics_file",
                        tumorBamMetricsPath,
                        "-virus_breakend_tsv",
                        input.path(),
                        "-taxonomy_db_tsv",
                        resourceFiles.virusInterpreterTaxonomyDb(),
                        "-virus_interpretation_tsv",
                        resourceFiles.virusInterpretation(),
                        "-virus_blacklist_tsv",
                        resourceFiles.virusBlacklist(),
                        "-output_dir",
                        new File(output.path()).getParent())));
    }
}
