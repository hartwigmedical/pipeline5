package com.hartwig.batch.input;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class JsonInputParser implements InputParser {
    private String inputFilePath;
    private String billedProject;

    JsonInputParser(String inputFilePath, String billedProject) {
        this.inputFilePath = inputFilePath;
        this.billedProject = billedProject;
    }

    @Override
    public List<List<InputFileDescriptor>> parse() throws RuntimeException {
        try {
            FileInputStream stream = new FileInputStream(inputFilePath);
            com.fasterxml.jackson.core.JsonParser jsonParser = new com.fasterxml.jackson.core.JsonFactory().createParser(stream);
            ObjectMapper mapper = new ObjectMapper();
            JavaType javaType = mapper.getTypeFactory().constructCollectionType(List.class, Map.class);
            List<Map<String, String>> objects = mapper.readValue(jsonParser, javaType);
            List<List<InputFileDescriptor>> toReturn = new ArrayList<>();
            objects.forEach(o -> {
                List<InputFileDescriptor> fileDescriptors = new ArrayList<>();
                for (String key : o.keySet()) {
                    fileDescriptors.add(InputFileDescriptor.builder().name(key).billedProject(billedProject)
                            .remoteFilename(o.get(key)).build());
                }
                toReturn.add(fileDescriptors);
            });
            return toReturn;
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to parse inputs file", ioe);
        }
    }
}
