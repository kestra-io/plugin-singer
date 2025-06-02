package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Load data into a Snowflake database with a Singer target.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/MeltanoLabs/target-snowflake)."
)
public class MeltanoSnowflake extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Snowflake account name.",
        description = "(i.e. rtXXXXX.eu-central-1)"
    )
    @PluginProperty(dynamic = true)
    private String account;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The database name."
    )
    @PluginProperty(dynamic = true)
    private String database;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The database user."
    )
    @PluginProperty(dynamic = true)
    private String username;

    @Schema(
        title = "The database user's password."
    )
    private Property<String> password;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Snowflake virtual warehouse name."
    )
    @PluginProperty(dynamic = true)
    private String warehouse;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The database schema."
    )
    @PluginProperty(dynamic = true)
    private String schema;

    @Schema(
        title = "The initial role for the session."
    )
    private Property<String> role;

    @Schema(
        title = "Whether to add metadata columns."
    )
    @Builder.Default
    private Property<Boolean> addRecordMetadata = Property.ofValue(true);

    @Schema(
        title = "The default target database schema name to use for all streams."
    )
    private Property<String> defaultTargetSchema;

    @Schema(
        title = "'True' to enable schema flattening and automatically expand nested properties."
    )
    private Property<Boolean> flatteningEnabled;

    @Schema(
        title = "The max depth to flatten schemas."
    )
    private Property<Integer> flatteningMaxDepth;

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("account", runContext.render(this.account))
            .put("database", runContext.render(this.database))
            .put("username", runContext.render(this.username))
            .put("password", runContext.render(this.password).as(String.class).orElseThrow())
            .put("warehouse", runContext.render(this.warehouse))
            .put("schema", runContext.render(this.schema))
            .put("add_record_metadata", runContext.render(this.addRecordMetadata).as(Boolean.class).orElseThrow().toString());

        if (this.role != null) {
            builder.put("role", runContext.render(this.role).as(String.class).orElseThrow());
        }

        if (this.defaultTargetSchema != null) {
            builder.put("default_target_schema", runContext.render(this.defaultTargetSchema).as(String.class).orElseThrow());
        }

        if (this.flatteningEnabled != null) {
            builder.put("flattening_enabled", runContext.render(this.flatteningEnabled).as(Boolean.class).orElseThrow().toString());
        }

        if (this.flatteningMaxDepth != null) {
            builder.put("flattening_max_depth", runContext.render(this.flatteningMaxDepth).as(Integer.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("meltanolabs-target-snowflake"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("target-snowflake");
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.runTarget(runContext);
    }
}
