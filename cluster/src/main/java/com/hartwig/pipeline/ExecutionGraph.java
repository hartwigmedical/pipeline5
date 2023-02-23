package com.hartwig.pipeline;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import com.hartwig.pipeline.input.InputDependencyProvider;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.OutputClassUtil;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageRunner;

import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionGraph.class);

    private final Map<String, Stage<? extends StageOutput, SomaticRunMetadata>> stageByTag = new HashMap<>();
    private final Map<String, StageOutput> stageInputByTag = new HashMap<>();
    private final DefaultDirectedGraph<String, DefaultEdge> g = new DefaultDirectedGraph<>(DefaultEdge.class);
    private ExecutionGraphRun activeRun;

    void addStage(Stage<? extends StageOutput, SomaticRunMetadata> stage) {
        var tag = stage.outputClassTag();
        if (activeRun != null) {
            LOGGER.warn("Cannot add stage with tag '{}'. Run already started.", tag);
            return;
        }
        g.addVertex(tag);
        stageByTag.put(tag, stage);
    }

    @SafeVarargs
    final void addStages(Stage<? extends StageOutput, SomaticRunMetadata>... stages) {
        for (var stage : stages) {
            addStage(stage);
        }
    }

    void addStageInput(StageOutput input, Class<? extends StageOutput> stageOutputClass, String label) {
        var tag = OutputClassUtil.getOutputClassTag(stageOutputClass, label);
        if (activeRun != null) {
            LOGGER.warn("Cannot add stage input with tag '{}'. Run already started.", tag);
            return;
        }
        g.addVertex(tag);
        stageInputByTag.put(tag, input);
    }

    private void completeGraph() {
        var vertices = g.vertexSet();
        for (final Stage<? extends StageOutput, SomaticRunMetadata> stage : stageByTag.values()) {
            var tag = stage.outputClassTag();
            stage.registerInput(new InputDependencyProvider() {
                @Override
                public <T> T registerInput(Class<T> clazz) {
                    var classTag = OutputClassUtil.getOutputClassTag(clazz);
                    if (!vertices.contains(classTag)) {
                        throw new IllegalArgumentException(String.format("%s expects input %s, but this input does not exist.",
                                tag,
                                classTag));
                    }
                    g.addEdge(classTag, tag);
                    return null;
                }

                @Override
                public <T> T registerInput(Class<T> clazz, String label) {
                    var classTag = OutputClassUtil.getOutputClassTag(clazz, label);
                    if (!vertices.contains(classTag)) {
                        throw new IllegalArgumentException(String.format("%s expects input %s, but this input does not exist.",
                                tag,
                                classTag));
                    }
                    g.addEdge(classTag, tag);
                    return null;
                }
            }, false);
        }

        // TODO: Some checks on validity of the graph

        LOGGER.info("Completed execution graph. Looks like: {}", exportAsDot());
    }

    Future<PipelineState> run(ExecutorService executorService, StageRunner<SomaticRunMetadata> stageRunner, SomaticRunMetadata metadata,
            PipelineResults pipelineResults) {
        this.completeGraph();
        activeRun = new ExecutionGraphRun(g, stageInputByTag, executorService, stageRunner, metadata, pipelineResults);
        return activeRun.start();
    }

    String exportAsDot() {
        Set<String> runningTags = Optional.ofNullable(activeRun).map(ExecutionGraphRun::getRunningTags).orElseGet(HashSet::new);
        Set<String> finishedTags = Optional.ofNullable(activeRun).map(ExecutionGraphRun::getFinishedTags).orElseGet(HashSet::new);
        var exporter = new DOTExporter<String, DefaultEdge>();
        exporter.setVertexAttributeProvider((v) -> {
            Map<String, Attribute> map = new LinkedHashMap<>();
            map.put("label", DefaultAttribute.createAttribute(v));
            if (runningTags.contains(v)) {
                map.put("color", DefaultAttribute.createAttribute("red"));
            } else if (finishedTags.contains(v)) {
                map.put("color", DefaultAttribute.createAttribute("green"));
            }
            return map;
        });
        var writer = new StringWriter();
        exporter.exportGraph(g, writer);
        return writer.toString();
    }

    private class ExecutionGraphRun {

        private final ExecutorService executorService;
        private final StageRunner<SomaticRunMetadata> stageRunner;
        private final SomaticRunMetadata metadata;

        private final PipelineResults pipelineResults;
        private final PipelineState state = new PipelineState();
        private final DefaultDirectedGraph<String, DefaultEdge> runGraph;
        private final Map<String, StageOutput> completeStagesByTag;
        private final Set<String> runningStageTags = new HashSet<>();
        private final BlockingQueue<Pair<String, StageOutput>> stageDoneQueue = new LinkedBlockingQueue<>();

        ExecutionGraphRun(DefaultDirectedGraph<String, DefaultEdge> g, Map<String, StageOutput> stageInputByTag,
                ExecutorService executorService, StageRunner<SomaticRunMetadata> stageRunner, SomaticRunMetadata metadata,
                PipelineResults pipelineResults) {
            runGraph = (DefaultDirectedGraph<String, DefaultEdge>) g.clone();
            completeStagesByTag = new HashMap<>(stageInputByTag);
            completeStagesByTag.keySet().forEach(runGraph::removeVertex);
            this.executorService = executorService;
            this.stageRunner = stageRunner;
            this.metadata = metadata;
            this.pipelineResults = pipelineResults;
        }

        Future<PipelineState> start() {
            return this.executorService.submit(() -> {
                while (!runGraph.vertexSet().isEmpty() && state.shouldProceed()) {
                    runRound();
                    try {
                        var done = stageDoneQueue.take();
                        onStageDone(done.getLeft(), done.getRight());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                LOGGER.info("Finished running execution graph.");
                return state;
            });
        }

        private void runRound() {
            var readyStages = runGraph.vertexSet().stream()
                    .filter(tag -> runGraph.incomingEdgesOf(tag).size() == 0)
                    .filter(tag -> !runningStageTags.contains(tag))
                    .collect(Collectors.toList());
            for (String root : readyStages) {
                LOGGER.info("Starting graph vertex {}", root);
                Stage<? extends StageOutput, SomaticRunMetadata> stage = stageByTag.get(root);
                stage.registerInput(new InputDependencyProvider() {
                    @Override
                    public <T> T registerInput(final Class<T> clazz) {
                        var tag = OutputClassUtil.getOutputClassTag(clazz);
                        return (T) completeStagesByTag.get(tag);
                    }

                    @Override
                    public <T> T registerInput(final Class<T> clazz, final String label) {
                        var tag = OutputClassUtil.getOutputClassTag(clazz, label);
                        return (T) completeStagesByTag.get(tag);
                    }
                }, true);
                var tag = stage.outputClassTag();
                runningStageTags.add(tag);
                executorService.submit(() -> {
                    StageOutput result = stageRunner.run(metadata, stage);
                    stageDoneQueue.add(Pair.of(tag, result));
                });
            }
        }

        private void onStageDone(String tag, StageOutput stageOutput) {
            LOGGER.info("Finished running graph vertex {}", tag);
            pipelineResults.add(state.add(stageOutput));
            runningStageTags.remove(tag);
            completeStagesByTag.put(tag, stageOutput);
            runGraph.removeVertex(tag);
        }

        Set<String> getRunningTags() {
            return runningStageTags;
        }

        Set<String> getFinishedTags() {
            return completeStagesByTag.keySet();
        }
    }
}
