package com.didichuxing.datachannel.arius.fastindex.embeddedes;

import java.util.Collection;
import java.util.Map;

public class TemplatesDescription {
    private final Map<String, String> templates;

    TemplatesDescription(Map<String, String> templates) {
        this.templates = templates;
    }

    Collection<String> getTemplatesNames() {
        return templates.keySet();
    }

    String getTemplateSettings(String templateName) {
        return templates.get(templateName);
    }
}
