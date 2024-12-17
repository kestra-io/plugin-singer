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
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "A Generic Singer tap."
)
public class GenericTap extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @Schema(
        title = "The list of pip package to install."
    )
    private Property<List<String>> pipPackages;

    @NotNull
    @Schema(
        title = "The command to start."
    )
    private Property<String> command;

    @NotNull
    @Schema(
        title = "The list of feature the connector supports."
    )
    @Builder.Default
    private List<Feature> features = Arrays.asList(
        Feature.PROPERTIES,
        Feature.DISCOVER,
        Feature.STATE
    );

    @NotNull
    @Schema(
        title = "The configuration to use",
        description = "Will be save on config.json and used as arguments"
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> configs;

    public List<Feature> features() {
        return this.features;
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(configs);
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
