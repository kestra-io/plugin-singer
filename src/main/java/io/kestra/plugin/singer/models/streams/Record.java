package io.kestra.plugin.singer.models.streams;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.taps.AbstractPythonTap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@SuperBuilder
@Getter
@NoArgsConstructor
public class Record extends AbstractStream {
    private Map<String, Object> record;

    @JsonProperty("time_extracted")
    private Instant timeExtracted;

    @Getter(AccessLevel.NONE)
    Map<String, Object> extra;

    @JsonAnyGetter
    public Map<String, Object> getExtraFields(){
        return extra;
    }

    @JsonAnySetter
    public void setExtra(String name, Object value) {
        if (extra == null) {
            extra = new HashMap<>();
        }

        extra.put(name, value);
    }

    @Override
    public void onNext(RunContext runContext, AbstractPythonTap abstractPythonSinger) throws Exception {
        abstractPythonSinger.recordMessage(runContext, stream, record);
    }
}
