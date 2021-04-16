package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.List;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.tools.Versions;

public class RepeatMasker extends SubStage {

    public RepeatMasker() {
        super("gridss.repeatmasker", FileTypes.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        String repeatMasker = VmDirectories.TOOLS + "/repeatmasker/" + Versions.REPEAT_MASKER + "/RepeatMasker";
        return List.of(new VersionedToolCommand("gridss",
                "gridss_annotate_vcf_repeatmasker.sh",
                Versions.GRIDSS,
                "--output",
                output.path(),
                "--jar",
                GridssJar.path(),
                "-w",
                VmDirectories.OUTPUT,
                "--rm",
                repeatMasker,
                input.path()));
    }
}
