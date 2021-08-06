package io.kestra.plugin.singer.models.streams;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.taps.AbstractPythonTap;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

@SuperBuilder
@Getter
@NoArgsConstructor
public class Schema extends AbstractStream {
    private Map<String, Object> schema;

    @JsonProperty("key_properties")
    private List<String> keyProperties;

    @JsonProperty("bookmark_properties")
    private List<String> bookmarkProperties;

    @Override
    public void onNext(RunContext runContext, AbstractPythonTap abstractPythonSinger) {
        abstractPythonSinger.schemaMessage(this.stream, this.schema);
    }
}
