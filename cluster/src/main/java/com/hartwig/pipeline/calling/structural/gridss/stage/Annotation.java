package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.List;

import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateVariants;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.immutables.value.Value;

public class Annotation {
    private final CommandFactory commandFactory;

    @Value.Immutable
    public interface AnnotationResult {
        String annotatedVcf();

        List<BashCommand> commands();
    }

    public Annotation(final CommandFactory commandFactory) {
        this.commandFactory = commandFactory;
    }

    public AnnotationResult initialise(final String sampleBam, final String tumorBam, final String assemblyBam, final String rawVcf,
            final String referenceGenome, final String tumorSampleName) {
        AnnotateVariants variants =
                commandFactory.buildAnnotateVariants(sampleBam, tumorBam, assemblyBam, rawVcf, referenceGenome, tumorSampleName);
        AnnotateUntemplatedSequence untemplated =
                commandFactory.buildAnnotateUntemplatedSequence(variants.resultantVcf(), referenceGenome, tumorSampleName);
        BgzipCommand bgzip = commandFactory.buildBgzipCommand(untemplated.resultantVcf());
        String finalOutputPath = format("%s.gz", untemplated.resultantVcf());
        TabixCommand tabix = commandFactory.buildTabixCommand(finalOutputPath);

        return ImmutableAnnotationResult.builder()
                .annotatedVcf(finalOutputPath)
                .commands(asList(variants, untemplated, bgzip, tabix))
                .build();
    }
}
