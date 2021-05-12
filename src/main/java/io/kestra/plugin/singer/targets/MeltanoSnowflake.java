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
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "A Singer target loads data into a Snowflake database.",
    description = "Full documentation can be found [here](https://github.com/transferwise/pipelinewise-target-snowflake)"
)
public class MeltanoSnowflake extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Snowflake account name",
        description = "(i.e. rtXXXXX.eu-central-1)"
    )
    @PluginProperty(dynamic = true)
    private String account;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The database name"
    )
    @PluginProperty(dynamic = true)
    private String database;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The database user"
    )
    @PluginProperty(dynamic = true)
    private String username;

    @Schema(
        title = "The database user's password"
    )
    @PluginProperty(dynamic = true)
    private String password;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Snowflake virtual warehouse name"
    )
    @PluginProperty(dynamic = true)
    private String warehouse;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The database schema"
    )
    @PluginProperty(dynamic = true)
    private String schema;

    @Schema(
        title = "How many records are sent to Snowflake at a time?"
    )
    @PluginProperty(dynamic = false)
    private final Integer batchSize = 5000;

    @Schema(
        title = "Name of the column used for recording the timestamp when Data are uploaded to Snowflake."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String timestampColumn = "__loaded_at";

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("account", runContext.render(this.account))
            .put("database", runContext.render(this.database))
            .put("username", runContext.render(this.username))
            .put("password", runContext.render(this.password))
            .put("warehouse", runContext.render(this.warehouse))
            .put("schema", runContext.render(this.schema))
            .put("timestamp_column", runContext.render(this.timestampColumn));

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("git+https://gitlab.com/meltano/target-snowflake.git");
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
