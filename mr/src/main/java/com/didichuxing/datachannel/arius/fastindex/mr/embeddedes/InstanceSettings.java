package com.didichuxing.datachannel.arius.fastindex.mr.embeddedes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class InstanceSettings {

    private final ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());
    private final Map<String, Object> settings;

    InstanceSettings() {
        this(Collections.emptyMap());
    }

    InstanceSettings(Map<String, Object> settings) {
        this.settings = Collections.unmodifiableMap(settings);
    }

    InstanceSettings withSetting(String key, Object value) {
        Map<String, Object> extendedSettings = new HashMap<>();
        extendedSettings.putAll(settings);
        extendedSettings.put(key, value);
        return new InstanceSettings(extendedSettings);
    }

    String toYaml() {
        try {
            return yamlObjectMapper.writeValueAsString(settings);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
