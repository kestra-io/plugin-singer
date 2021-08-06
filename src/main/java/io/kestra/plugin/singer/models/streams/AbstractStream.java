package io.kestra.plugin.singer.models.streams;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.taps.AbstractPythonTap;
import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", visible = true, include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(value = Schema.class, name = "SCHEMA"),
    @JsonSubTypes.Type(value = State.class, name = "STATE"),
    @JsonSubTypes.Type(value = ActivateVersion.class, name = "ACTIVATE_VERSION"),
    @JsonSubTypes.Type(value = Record.class, name = "RECORD")
})
@Getter
@NoArgsConstructor
@Introspected
@SuperBuilder
public abstract class AbstractStream {
    @NotNull
    protected Type type;

    @NotNull
    protected String stream;

    public enum Type {
        SCHEMA,
        STATE,
        ACTIVATE_VERSION,
        RECORD,
    }

    abstract public void onNext(RunContext runContext, AbstractPythonTap abstractPythonSinger) throws Exception;
}
