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

    public VirusInterpreter(final String sampleId, final ResourceFiles resourceFiles) {
        super("virus.annotated", FileTypes.TSV);
        this.sampleId = sampleId;
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return List.of(new JavaJarCommand("virus-interpreter",
                Versions.VIRUS_INTERPRETER,
                "virus-interpreter.jar",
                "2G",
                List.of("-sample_id",
                        sampleId,
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
