package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateVariants;
import com.hartwig.pipeline.calling.structural.gridss.command.GridssToBashCommandConverter;
import com.hartwig.pipeline.execution.vm.BashCommand;
import org.immutables.value.Value;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Annotation {
    private final CommandFactory commandFactory;
    private final GridssToBashCommandConverter converter;

    @Value.Immutable
    public interface AnnotationResult {
        String annotatedVcf();
        List<BashCommand> commands();
    }

    public Annotation(final CommandFactory commandFactory, final GridssToBashCommandConverter converter) {
        this.commandFactory = commandFactory;
        this.converter = converter;
    }

    public AnnotationResult initialise(final String sampleBam, final String tumorBam, final String assemblyBam,
                                       final String rawVcf, final String referenceGenome) {
        AnnotateVariants variants = commandFactory.buildAnnotateVariants(sampleBam, tumorBam, assemblyBam, rawVcf, referenceGenome);
        AnnotateUntemplatedSequence untemplated = commandFactory.buildAnnotateUntemplatedSequence(variants.resultantVcf(), referenceGenome);
        BgzipCommand bgzip = commandFactory.buildBgzipCommand(untemplated.resultantVcf());
        String finalOutputPath = format("%s.gz", untemplated.resultantVcf());
        TabixCommand tabix = commandFactory.buildTabixCommand(finalOutputPath);

        return ImmutableAnnotationResult.builder()
                .annotatedVcf(finalOutputPath)
                .commands(asList(converter.convert(variants), converter.convert(untemplated), bgzip, tabix))
                .build();
    }
}
