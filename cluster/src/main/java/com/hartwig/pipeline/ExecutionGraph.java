package com.hartwig.pipeline;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import com.hartwig.pipeline.input.InputDependencyProvider;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.OutputClassUtil;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageRunner;

import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionGraph.class);

    private final ExecutorService executorService;
    private final StageRunner<SomaticRunMetadata> stageRunner;

    private final Map<String, Stage<? extends StageOutput, SomaticRunMetadata>> stageByTag = new HashMap<>();
    private final Map<String, StageOutput> stageInputByTag = new HashMap<>();
    private final DefaultDirectedGraph<String, DefaultEdge> g = new DefaultDirectedGraph<>(DefaultEdge.class);

    public ExecutionGraph(final ExecutorService executorService, final StageRunner<SomaticRunMetadata> stageRunner) {
        this.executorService = executorService;
        this.stageRunner = stageRunner;
    }

    void addStage(Stage<? extends StageOutput, SomaticRunMetadata> stage) {
        var tag = stage.outputClassTag();
        g.addVertex(tag);
        stageByTag.put(tag, stage);
    }

    void addStages(List<Stage<? extends StageOutput, SomaticRunMetadata>> stages) {
        stages.forEach(this::addStage);
    }

    void addStageInput(StageOutput input, String label) {
        var tag = OutputClassUtil.getOutputClassTag(input.getClass(), label);
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

        LOGGER.info("Completed execution graph. Looks like: {}", exportAsDot());
    }

    void run() {
        PipelineState state = new PipelineState();
        DefaultDirectedGraph<String, DefaultEdge> runGraph = (DefaultDirectedGraph<String, DefaultEdge>) g.clone();
        Map<String, StageOutput> completeStagesByTag = new HashMap<>(stageInputByTag);
        completeStagesByTag.keySet().forEach(runGraph::removeVertex);
        Map<String, StageOutput> runningStagesByTag = new HashMap<>();

    }

    String exportAsDot() {
        var exporter = new DOTExporter<String, DefaultEdge>();
        exporter.setVertexAttributeProvider((v) -> {
            Map<String, Attribute> map = new LinkedHashMap<>();
            map.put("label", DefaultAttribute.createAttribute(v));
            return map;
        });
        var writer = new StringWriter();
        exporter.exportGraph(g, writer);
        return writer.toString();
    }

    class ExecutionGraphRun {

        private final ExecutorService executorService;
        private final StageRunner<SomaticRunMetadata> stageRunner;
        private final SomaticRunMetadata metadata;

        private final PipelineResults pipelineResults;
        private final PipelineState state = new PipelineState();
        private final DefaultDirectedGraph<String, DefaultEdge> runGraph;
        private final Map<String, StageOutput> completeStagesByTag;
        private final Set<String> runningStageTags = new HashSet<>();

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

        void start() {
            this.executorService.submit(() -> {
                runRound();

            })
        }

        void runRound() {
            synchronized (this) {
                var readyStages = runGraph.vertexSet().stream()
                        .filter(key -> runGraph.incomingEdgesOf(key).size() == 0)
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
                    executorService.submit(() -> submitStage(stage, tag));
                }
            }
        }

        private void submitStage(final Stage<? extends StageOutput, SomaticRunMetadata> stage, final String tag) {
            var stageOutput = stageRunner.run(metadata, stage);
            LOGGER.info("Finished running graph vertex {}", tag);
            synchronized (this) {
                pipelineResults.add(state.add(stageOutput));
                runningStageTags.remove(tag);
                completeStagesByTag.put(tag, stageOutput);
                runGraph.removeVertex(tag);
            }
            runRound();
        }
    }
}
