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
    title = "A Singer target loads data into a Postgres database.",
    description = "Full documentation can be found [here](https://github.com/datamill-co/target-postgres)"
)
public class DatamillCoPostgres extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
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
        title = "The database user"
    )
    @PluginProperty(dynamic = true)
    private String username;

    @Schema(
        title = "The database user's password"
    )
    @PluginProperty(dynamic = true)
    private String password;

    @Schema(
        title = "The database name"
    )
    @PluginProperty(dynamic = true)
    private String dbName;

    @NotNull
    @Schema(
        title = "The database port"
    )
    @PluginProperty(dynamic = false)
    private Integer port;

    @Schema(
        title = "The database schema"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String schema = "public";

    @Schema(
        title = "Refer to the [libpq](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS) docs for more information about SSL"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String sslMode = "prefer";

    @Schema(
        title = "Crash on invalid records."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Boolean invalidRecordsDetect = true;

    @Schema(
        title = "Include a positive value n in your config to allow to encounter at most n invalid records per stream before giving up."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Integer invalidRecordsThreshold = 0;

    @Schema(
        title = "The level for logging.",
        description = "Set to DEBUG to get things like queries executed, timing of those queries, etc. See Python's Logger Levels for information about valid values."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String loggingLevel = "INFO";

    @Schema(
        title = "Whether the Target should create tables which have no records present in Remote."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Boolean persistEmptyTables = false;

    @Schema(
        title = "The maximum number of rows to buffer in memory before writing to the destination table in Postgres."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Integer maxBatchRows = 200000;

    @Schema(
        title = "The maximum number of bytes to buffer in memory before writing to the destination table in Postgres."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Integer maxBufferSize = 104857600;

    @Schema(
        title = "How often, in rows received, to count the buffered rows and bytes to check if a flush is necessary.",
        description = "There's a slight performance penalty to checking the buffered records count or bytesize, so this" +
            " controls how often this is polled in order to mitigate the penalty. This value is usually not necessary" +
            " to set as the default is dynamically adjusted to check reasonably often. \n\n" +
            "Default is 5000, or 1/40th `maxBatchRows`"
    )
    @PluginProperty(dynamic = false)
    private Integer batchDetectionThreshold;

    @Schema(
        title = "Whether the Target should create column indexes on the important columns used during data loading.",
        description = "These indexes will make data loading slightly slower but the deduplication phase much faster. Defaults to on for better baseline performance."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private final Boolean addUpsertIndexes = true;

    @Schema(
        title = "Raw SQL statement(s) to execute as soon as the connection to Postgres is opened by the target.",
        description = "Useful for setup like SET ROLE or other connection state that is important."
    )
    @PluginProperty(dynamic = false)
    private String beforeRunSql;

    @Schema(
        title = "Raw SQL statement(s) to before closing the connection to Postgres."
    )
    @PluginProperty(dynamic = false)
    private String afterRunSql;

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("postgres_username", runContext.render(this.username))
            .put("postgres_password", runContext.render(this.password))
            .put("postgres_host", runContext.render(this.host))
            .put("postgres_port", this.port)
            .put("postgres_database", runContext.render(this.dbName))
            .put("postgres_schema", runContext.render(this.schema))
            .put("postgres_sslmode", this.sslMode)
            .put("invalid_records_detect", this.invalidRecordsDetect)
            .put("invalid_records_threshold", this.invalidRecordsThreshold)
            .put("disable_collection", true)
            .put("logging_level", this.loggingLevel)
            .put("persist_empty_tables", this.persistEmptyTables)
            .put("max_batch_rows", this.maxBatchRows)
            .put("max_buffer_size", this.maxBufferSize)
            .put("state_support", true)
            .put("add_upsert_indexes", this.addUpsertIndexes);

        if (this.batchDetectionThreshold != null) {
            builder.put("batch_detection_threshold", this.batchDetectionThreshold);
        }

        if (this.beforeRunSql != null) {
            builder.put("before_run_sql", runContext.render(this.beforeRunSql));
        }

        if (this.afterRunSql != null) {
            builder.put("after_run_sql", runContext.render(this.afterRunSql));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("singer-target-postgres");
    }

    @Override
    protected String command() {
        return "target-postgres";
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.runTarget(runContext);
    }
}
