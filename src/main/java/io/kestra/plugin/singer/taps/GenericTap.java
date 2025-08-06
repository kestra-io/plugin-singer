package io.kestra.plugin.singer.taps;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(deprecated = true,
    title = "Fetch data using a generic Singer tap."
)
@Deprecated(forRemoval = true, since = "0.24")
public class GenericTap extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @Schema(deprecated = true,
        title = "The list of pip package to install."
    )
    private Property<List<String>> pipPackages;

    @NotNull
    @Schema(deprecated = true,
        title = "The command to start."
    )
    private Property<String> command;

    @NotNull
    @Schema(deprecated = true,
        title = "The list of feature the connector supports."
    )
    @Builder.Default
    private List<Feature> features = Arrays.asList(
        Feature.PROPERTIES,
        Feature.DISCOVER,
        Feature.STATE
    );

    @NotNull
    @Schema(deprecated = true,
        title = "The configuration to use",
        description = "Will be save on config.json and used as arguments"
    )
    private Property<Map<String, Object>> configs;

    public List<Feature> features() {
        return this.features;
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        var config = runContext.render(configs).asMap(String.class, Object.class);
        return config.isEmpty() ? new HashMap<>() : config;
    }

    @Override
    public Property<List<String>> pipPackages() {
        return this.pipPackages;
    }

    @Override
    protected Property<String> command() {
        return this.command;
    }
}
