package com.hartwig.pipeline;

import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.OutputClassUtil;
import com.hartwig.pipeline.stages.Stage;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import java.util.ArrayList;
import java.util.List;

public class PipelineGraph {
    private final List<PipelineNode> nodes = new ArrayList<>();
    private final DefaultDirectedGraph<String, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);

    void addStage(Stage<? extends StageOutput, SomaticRunMetadata> stage) {
        var tag = stage.outputClassTag();
        var stageNode = ImmutablePipelineNode.builder()
                .stage(stage)
                .tag(tag)
                .build();
        nodes.add(stageNode);
        graph.addVertex(tag);
    }

    void addPipelineInput(StageOutput stageOutput, String label) {
        var tag = OutputClassUtil.getOutputClassTag(stageOutput.getClass(), label);
        var inputNode = ImmutablePipelineNode.builder()
                .stageOutput(stageOutput)
                .tag(tag)
                .build();
        nodes.add(inputNode);
        graph.addVertex(tag);
    }

    void completeGraph() {

    }
}
