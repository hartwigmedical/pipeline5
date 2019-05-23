package com.hartwig.pipeline.tertiary.healthcheck;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashCommand;

public class GenerateMetadataCommand implements BashCommand {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private String inputDirectory;
    private final String setName;
    private final AlignmentPair pair;

    GenerateMetadataCommand(final String inputDirectory, final String setName, final AlignmentPair pair) {
        this.inputDirectory = inputDirectory;
        this.setName = setName;
        this.pair = pair;
    }

    @Override
    public String asBash() {
        try {
            return String.format("echo '%s' | tee %s/metadata",
                    OBJECT_MAPPER.writeValueAsString(Pv4Metadata.builder()
                            .ref_sample(pair.reference().sample().name())
                            .tumor_sample(pair.tumor().sample().name())
                            .set_name(setName)
                            .build()),
                    inputDirectory);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
