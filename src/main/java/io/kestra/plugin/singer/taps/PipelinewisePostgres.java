package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "A Singer tap to fetch data from a Postgres database.",
    description = "Full documentation can be found [here](https://github.com/transferwise/pipelinewise-tap-postgres)"
)
public class PipelinewisePostgres extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "The database hostname"
    )
    @PluginProperty(dynamic = true)
    private String host;

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

    @Schema(
        title = "The database name."
    )
    @PluginProperty(dynamic = true)
    private String dbName;

    @NotNull
    @Schema(
        title = "The database port."
    )
    @PluginProperty
    private Integer port;

    @Schema(
        title = "If ssl is enabled."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean ssl = false;

    @Schema(
        title = "Stop running the tap when no data received from wal after certain number of seconds."
    )
    @PluginProperty
    @Builder.Default
    private final Integer logicalPollSeconds = 10800;

    @Schema(
        title = "Stop running the tap if the newly received lsn is after the max lsn that was detected when the tap started."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean breakAtEndLsn = true;

    @Schema(
        title = "Stop running the tap after certain number of seconds."
    )
    @PluginProperty
    @Builder.Default
    private final Integer maxRunSeconds = 43200;

    @Schema(
        title = "If set to \"true\" then add _sdc_lsn property to the singer messages to debug postgres LSN position in the WAL stream."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean debugLsn = false;

    @Schema(
        title = "The list of schemas to extract tables only from particular schemas and to improve data extraction performance"
    )
    @PluginProperty(dynamic = true)
    private List<String> filterSchemas;

    public List<Feature> features() {
        return Arrays.asList(
            Feature.CATALOG,
            Feature.DISCOVER,
            Feature.STATE
        );
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("user", runContext.render(this.username))
            .put("password", runContext.render(this.password))
            .put("host", runContext.render(this.host))
            .put("port", this.port)
            .put("dbname", runContext.render(this.dbName))
            .put("ssl", this.ssl)
            .put("logical_poll_seconds", this.logicalPollSeconds)
            .put("break_at_end_lsn", this.breakAtEndLsn)
            .put("max_run_seconds", this.maxRunSeconds)
            .put("debug_lsn", this.debugLsn);

        if (this.filterSchemas != null) {
            builder.put("filter_dbs", String.join(",", runContext.render(this.filterSchemas)));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return List.of("pipelinewise-tap-postgres");
    }

    @Override
    protected String command() {
        return "tap-postgres";
    }
}
