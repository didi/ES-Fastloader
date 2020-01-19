package com.didichuxing.datachannel.arius.fastindex.embeddedes;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

class IndicesDescription {

    private final Map<String, Optional<IndexSettings>> indices;

    IndicesDescription(Map<String, Optional<IndexSettings>> indices) {
        this.indices = indices;
    }

    Collection<String> getIndicesNames() {
        return indices.keySet();
    }

    Optional<IndexSettings> getIndexSettings(String indexName) {
        return indices.get(indexName);
    }
}
