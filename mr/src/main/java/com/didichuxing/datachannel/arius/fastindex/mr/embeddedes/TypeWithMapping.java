package com.didichuxing.datachannel.arius.fastindex.mr.embeddedes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class TypeWithMapping {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String type;
    private final JsonNode mapping;

    public TypeWithMapping(String type, String mapping) {
        this.type = type;

        try {
            JsonNode json = OBJECT_MAPPER.readTree(mapping);
            this.mapping = json.get(type);
        } catch (IOException e) {
            throw new RuntimeException("Problem with provided mapping for type: " + type, e);
        }
    }

    public String getType() {
        return type;
    }

    public JsonNode getMapping() {
        return mapping;
    }
}
