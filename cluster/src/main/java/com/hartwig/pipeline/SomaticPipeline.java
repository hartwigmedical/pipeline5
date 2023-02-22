package com.hartwig.pipeline;

import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.sage.SageGermlineCaller;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.calling.sage.SageSomaticCaller;
import com.hartwig.pipeline.calling.structural.gridss.Gridss;
import com.hartwig.pipeline.calling.structural.gridss.GridssOutput;
import com.hartwig.pipeline.calling.structural.gripss.GripssGermline;
import com.hartwig.pipeline.calling.structural.gripss.GripssOutput;
import com.hartwig.pipeline.calling.structural.gripss.GripssSomatic;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.input.InputDependencyProvider;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.output.OutputClassUtil;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.chord.Chord;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.cuppa.Cuppa;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.lilac.Lilac;
import com.hartwig.pipeline.tertiary.lilac.LilacBamSliceOutput;
import com.hartwig.pipeline.tertiary.lilac.LilacBamSlicer;
import com.hartwig.pipeline.tertiary.lilac.LilacOutput;
import com.hartwig.pipeline.tertiary.linx.LinxGermline;
import com.hartwig.pipeline.tertiary.linx.LinxGermlineOutput;
import com.hartwig.pipeline.tertiary.linx.LinxSomatic;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutput;
import com.hartwig.pipeline.tertiary.orange.Orange;
import com.hartwig.pipeline.tertiary.orange.OrangeOutput;
import com.hartwig.pipeline.tertiary.pave.PaveGermline;
import com.hartwig.pipeline.tertiary.pave.PaveOutput;
import com.hartwig.pipeline.tertiary.pave.PaveSomatic;
import com.hartwig.pipeline.tertiary.peach.Peach;
import com.hartwig.pipeline.tertiary.peach.PeachOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.sigs.Sigs;
import com.hartwig.pipeline.tertiary.sigs.SigsOutput;
import com.hartwig.pipeline.tertiary.virus.VirusBreakend;
import com.hartwig.pipeline.tertiary.virus.VirusBreakendOutput;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreter;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreterOutput;

import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SomaticPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(SomaticPipeline.class);

    private final Arguments arguments;
    private final StageRunner<SomaticRunMetadata> stageRunner;
    private final BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue;
    private final BlockingQueue<BamMetricsOutput> tumorBamMetricsOutputQueue;
    private final BlockingQueue<FlagstatOutput> referenceFlagstatOutputQueue;
    private final BlockingQueue<FlagstatOutput> tumorFlagstatOutputQueue;
    private final SomaticRunMetadata metadata;
    private final PipelineResults pipelineResults;
    private final ExecutorService executorService;
    private final PersistedDataset persistedDataset;

    SomaticPipeline(final Arguments arguments, final StageRunner<SomaticRunMetadata> stageRunner,
            final BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue,
            final BlockingQueue<BamMetricsOutput> tumorBamMetricsOutputQueue,
            final BlockingQueue<FlagstatOutput> referenceFlagstatOutputQueue, final BlockingQueue<FlagstatOutput> tumorFlagstatOutputQueue,
            final SomaticRunMetadata metadata, final PipelineResults pipelineResults, final ExecutorService executorService,
            final PersistedDataset persistedDataset) {
        this.arguments = arguments;
        this.stageRunner = stageRunner;
        this.referenceBamMetricsOutputQueue = referenceBamMetricsOutputQueue;
        this.tumorBamMetricsOutputQueue = tumorBamMetricsOutputQueue;
        this.referenceFlagstatOutputQueue = referenceFlagstatOutputQueue;
        this.tumorFlagstatOutputQueue = tumorFlagstatOutputQueue;
        this.metadata = metadata;
        this.pipelineResults = pipelineResults;
        this.executorService = executorService;
        this.persistedDataset = persistedDataset;
    }

    public PipelineState run(final AlignmentPair pair) {
        PipelineState state = new PipelineState();
        LOGGER.info("Pipeline5 somatic pipeline starting for set [{}]", metadata.set());

        BamMetricsOutput tumorMetrics = metadata.maybeTumor()
                .map(t -> pollOrThrow(tumorBamMetricsOutputQueue, "tumor metrics"))
                .orElse(skippedMetrics(metadata.sampleName()));
        BamMetricsOutput referenceMetrics = metadata.maybeReference()
                .map(t -> pollOrThrow(referenceBamMetricsOutputQueue, "reference metrics"))
                .orElse(skippedMetrics(metadata.sampleName()));
        FlagstatOutput tumorFlagstat = metadata.maybeTumor()
                .map(t -> pollOrThrow(tumorFlagstatOutputQueue, "tumor flagstat"))
                .orElse(skippedFlagstat(metadata.sampleName()));
        FlagstatOutput referenceFlagstat = metadata.maybeReference()
                .map(t -> pollOrThrow(referenceFlagstatOutputQueue, "reference flagstat"))
                .orElse(skippedFlagstat(metadata.sampleName()));

        final ResourceFiles resourceFiles = buildResourceFiles(arguments);

        List<Stage<? extends StageOutput, SomaticRunMetadata>> stages = List.of(
                new Amber(pair, resourceFiles, persistedDataset, arguments),
                new Cobalt(pair, resourceFiles, persistedDataset, arguments),
                new SageSomaticCaller(pair, persistedDataset, resourceFiles, arguments),
                new SageGermlineCaller(pair, persistedDataset, resourceFiles),
                new Gridss(pair, resourceFiles, persistedDataset),
                new VirusBreakend(pair, resourceFiles, persistedDataset),
                new PaveSomatic(resourceFiles, persistedDataset),
                new PaveGermline(resourceFiles, persistedDataset),
                new GripssSomatic(persistedDataset, resourceFiles, arguments),
                new GripssGermline(persistedDataset, resourceFiles),
                new Purple(resourceFiles, persistedDataset, arguments),
                new VirusInterpreter(pair, resourceFiles, persistedDataset),
                new HealthChecker(),
                new LilacBamSlicer(pair, resourceFiles, persistedDataset),
                new LinxSomatic(resourceFiles, persistedDataset),
                new LinxGermline(resourceFiles, persistedDataset),
                new Lilac(resourceFiles, persistedDataset),
                new Sigs(resourceFiles, persistedDataset),
                new Chord(arguments.refGenomeVersion(), persistedDataset),
                new Peach(resourceFiles, persistedDataset),
                new Cuppa(resourceFiles, persistedDataset),
                new Orange(resourceFiles)
        );

        DefaultDirectedGraph<String, DefaultEdge> g = new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);

        stages.stream()
                .map(Stage::outputClassTag)
                .forEach(g::addVertex);
        g.addVertex(OutputClassUtil.getOutputClassTag(BamMetricsOutput.class, "tumor"));
        g.addVertex(OutputClassUtil.getOutputClassTag(BamMetricsOutput.class, "reference"));
        g.addVertex(OutputClassUtil.getOutputClassTag(FlagstatOutput.class, "tumor"));
        g.addVertex(OutputClassUtil.getOutputClassTag(FlagstatOutput.class, "reference"));

        var vertices = g.vertexSet();

        stages.forEach(stage -> stage.registerInput(new InputDependencyProvider() {
            @Override
            public <T> T registerInput(Class<T> clazz) {
                var classTag = OutputClassUtil.getOutputClassTag(clazz);
                if (!vertices.contains(classTag)) {
                    throw new IllegalArgumentException(String.format("%s expects input %s, but this input does not exist.", stage.outputClassTag(), classTag));
                }
                g.addEdge(classTag, stage.outputClassTag());
                return null;
            }

            @Override
            public <T> T registerInput(Class<T> clazz, String label) {
                var classTag = OutputClassUtil.getOutputClassTag(clazz, label);
                if (!vertices.contains(classTag)) {
                    throw new IllegalArgumentException(String.format("%s expects input %s, but this input does not exist.", stage.outputClassTag(), classTag));
                }
                g.addEdge(classTag, stage.outputClassTag());
                return null;
            }
        }, false));

        var exporter = new DOTExporter<String, DefaultEdge>();
        exporter.setVertexAttributeProvider((v) -> {
            Map<String, Attribute> map = new LinkedHashMap<>();
            map.put("label", DefaultAttribute.createAttribute(v));
            return map;
        });
        var writer = new StringWriter();
        exporter.exportGraph(g, writer);
        LOGGER.info("Somatic pipeline graph looks like: {}", writer);

        var topoGroups = topoGroups(g);

        pipelineResults.compose(metadata, "Somatic");
        return state;
    }

    static <T> List<List<T>> topoGroups(DefaultDirectedGraph<T, DefaultEdge> graph) {
        var inDegrees = graph.vertexSet().stream()
                .map(vertex -> Pair.of(vertex, graph.inDegreeOf(vertex)))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

        List<List<T>> result = new ArrayList<>();

        do {
            var zeroIn = inDegrees.entrySet().stream()
                    .filter(entry -> entry.getValue() == 0)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            zeroIn.forEach(vertex -> {
                inDegrees.remove(vertex);
                graph.outgoingEdgesOf(vertex).stream()
                        .map(graph::getEdgeTarget)
                        .forEach(outVertex -> inDegrees.computeIfPresent(outVertex, (v, degree) -> degree - 1));
            });
            result.add(zeroIn);
        } while (!inDegrees.isEmpty());

        return result;
    }

    private BamMetricsOutput skippedMetrics(final String sample) {
        return BamMetricsOutput.builder().sample(sample).status(PipelineStatus.SKIPPED).build();
    }

    private FlagstatOutput skippedFlagstat(final String sample) {
        return FlagstatOutput.builder().sample(sample).status(PipelineStatus.SKIPPED).build();
    }

    public static <T> T pollOrThrow(final BlockingQueue<T> queue, final String name) {
        try {
            T poll = queue.poll(24, TimeUnit.HOURS);
            if (poll == null) {
                throw new RuntimeException(String.format("No results from single sample pipeline within 24 hours for [%s]", name));
            }
            return poll;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
