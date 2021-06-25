package com.hartwig.pipeline.tertiary.virus;

import java.util.List;

import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.ExportPathCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.tools.Versions;

public class VirusBreakend extends SubStage {

    private final String tumorSample;
    private final String tumorBamPath;
    private final ResourceFiles resourceFiles;

    public VirusBreakend(final String tumorSample, final String tumorBamPath, final ResourceFiles resourceFiles) {
        super("virusbreakend.vcf.summary", FileTypes.TSV);
        this.tumorSample = tumorSample;
        this.tumorBamPath = tumorBamPath;
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return List.of(new ExportPathCommand(VmDirectories.toolPath("gridss/" + Versions.VIRUSBREAKEND_GRIDSS)),
                new ExportPathCommand(VmDirectories.toolPath("repeatmasker/" + Versions.REPEAT_MASKER)),
                new ExportPathCommand(VmDirectories.toolPath("kraken2/" + Versions.KRAKEN)),
                new ExportPathCommand(VmDirectories.toolPath("samtools/" + Versions.SAMTOOLS)),
                new ExportPathCommand(VmDirectories.toolPath("bcftools/" + Versions.BCF_TOOLS)),
                new ExportPathCommand(VmDirectories.toolPath("bwa/" + Versions.BWA)),
                new VirusBreakendCommand(resourceFiles, tumorSample, tumorBamPath));
    }
}
