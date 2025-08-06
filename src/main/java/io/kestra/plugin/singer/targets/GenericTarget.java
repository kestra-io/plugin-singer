package io.kestra.plugin.singer.targets;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(deprecated = true,
    title = "Load data using a generic Singer target."
)
@Deprecated(forRemoval = true, since="0.24")
public class GenericTarget extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
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
        title = "The configuration to use",
        description = "Will be save on config.json and used as arguments"
    )
    @PluginProperty(dynamic = true)
    private Property<Map<String, Object>> configs;

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return runContext.render(this.configs).asMap(String.class, Object.class);
    }

    @Override
    public Property<List<String>> pipPackages() {
        return this.pipPackages;
    }

    @Override
    protected Property<String> command() {
        return this.command;
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.runTarget(runContext);
    }
}
