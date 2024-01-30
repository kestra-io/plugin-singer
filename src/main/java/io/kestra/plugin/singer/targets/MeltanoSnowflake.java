package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "A Singer target loads data into a Snowflake database.",
    description = "Full documentation can be found [here](https://github.com/MeltanoLabs/target-snowflake)"
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
    @PluginProperty(dynamic = true)
    private String password;

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
    @PluginProperty(dynamic = true)
    private String role;

    @Schema(
        title = "Whether to add metadata columns."
    )
    @PluginProperty
    @Builder.Default
    private Boolean addRecordMetadata = true;

    @Schema(
        title = "The default target database schema name to use for all streams."
    )
    @PluginProperty(dynamic = true)
    private String defaultTargetSchema;

    @Schema(
        title = "'True' to enable schema flattening and automatically expand nested properties."
    )
    @PluginProperty
    private Boolean flatteningEnabled;

    @Schema(
        title = "The max depth to flatten schemas."
    )
    @PluginProperty
    private Integer flatteningMaxDepth;

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("account", runContext.render(this.account))
            .put("database", runContext.render(this.database))
            .put("username", runContext.render(this.username))
            .put("password", runContext.render(this.password))
            .put("warehouse", runContext.render(this.warehouse))
            .put("schema", runContext.render(this.schema))
            .put("add_record_metadata", this.addRecordMetadata.toString());

        if (this.role != null) {
            builder.put("role", runContext.render(this.role));
        }

        if (this.defaultTargetSchema != null) {
            builder.put("default_target_schema", runContext.render(this.defaultTargetSchema));
        }

        if (this.flatteningEnabled != null) {
            builder.put("flattening_enabled", this.flatteningEnabled.toString());
        }

        if (this.flatteningMaxDepth != null) {
            builder.put("flattening_max_depth", this.flatteningMaxDepth);
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("meltanolabs-target-snowflake");
    }

    @Override
    protected String command() {
        return "target-snowflake";
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.runTarget(runContext);
    }
}
