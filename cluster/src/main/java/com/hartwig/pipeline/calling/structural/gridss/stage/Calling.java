package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.IdentifyVariants;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class Calling extends SubStage {
    private final String referenceBam;
    private final String tumorBam;
    private final String referenceGenomePath;
    private final String configurationFile;
    private final String blacklist;

    public Calling(String referenceBam, String tumorBam, String referenceGenomePath, String configurationFile, String blacklist) {
        super("calling", OutputFile.VCF);
        this.referenceBam = referenceBam;
        this.tumorBam = tumorBam;
        this.referenceGenomePath = referenceGenomePath;
        this.configurationFile = configurationFile;
        this.blacklist = blacklist;
    }

    @Override
    public List<BashCommand> bash(OutputFile input, OutputFile output) {
        return Collections.singletonList(new IdentifyVariants(referenceBam,
                tumorBam,
                input.path(),
                output.path(),
                referenceGenomePath,
                configurationFile,
                blacklist));
    }
}
