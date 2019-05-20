package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.process.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.calling.structural.gridss.process.AnnotateVariants;
import com.hartwig.pipeline.execution.vm.BashCommand;
import org.immutables.value.Value;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Annotation {
    private CommandFactory commandFactory;

    @Value.Immutable
    public interface AnnotationResult {
        String annotatedVcf();
        List<BashCommand> commands();
    }

    public Annotation(CommandFactory commandFactory) {
        this.commandFactory = commandFactory;
    }

    public AnnotationResult initialise(String sampleBam, String tumorBam, String assemblyBam, String rawVcf, String referenceGenome) {
        AnnotateVariants variants = commandFactory.buildAnnotateVariants(sampleBam, tumorBam, assemblyBam, rawVcf, referenceGenome);
        AnnotateUntemplatedSequence untemplated = commandFactory.buildAnnotateUntemplatedSequence(variants.resultantVcf(), referenceGenome);
        BgzipCommand bgzip = commandFactory.buildBgzipCommand(untemplated.resultantVcf());
        String finalOutputPath = format("%s.gz", untemplated.resultantVcf());
        TabixCommand tabix = commandFactory.buildTabixCommand(finalOutputPath);

        return ImmutableAnnotationResult.builder().annotatedVcf(finalOutputPath)
                .commands(asList(variants, untemplated, bgzip, tabix))
                .build();
    }
}
