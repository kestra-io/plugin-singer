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
    title = "Load data into a PostgreSQL database with a Singer target. ",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/datamill-co/target-postgres)."
)
public class DatamillCoPostgres extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "The database hostname."
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
    private Property<String> password;

    @Schema(
        title = "The database name."
    )
    private Property<String> dbName;

    @NotNull
    @Schema(
        title = "The database port"
    )
    private Property<Integer> port;

    @Schema(
        title = "The database schema."
    )
    @Builder.Default
    private final Property<String> schema = Property.ofValue("public");

    @Schema(
        title = "Refer to the [libpq](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS) docs for more information about SSL."
    )
    @Builder.Default
    private final Property<String> sslMode = Property.ofValue("prefer");

    @Schema(
        title = "Crash on invalid records."
    )
    @Builder.Default
    private final Property<Boolean> invalidRecordsDetect = Property.ofValue(true);

    @Schema(
        title = "Include a positive value n in your config to allow to encounter at most n invalid records per stream before giving up."
    )
    @Builder.Default
    private final Property<Integer> invalidRecordsThreshold = Property.ofValue(0);

    @Schema(
        title = "The level for logging.",
        description = "Set to DEBUG to get things like queries executed, timing of those queries, etc. See Python's Logger Levels for information about valid values."
    )
    @Builder.Default
    private final Property<String> loggingLevel = Property.ofValue("INFO");

    @Schema(
        title = "Whether the Target should create tables which have no records present in Remote."
    )
    @Builder.Default
    private final Property<Boolean> persistEmptyTables = Property.ofValue(false);

    @Schema(
        title = "The maximum number of rows to buffer in memory before writing to the destination table in Postgres."
    )
    @Builder.Default
    private final Property<Integer> maxBatchRows = Property.ofValue(200000);

    @Schema(
        title = "The maximum number of bytes to buffer in memory before writing to the destination table in Postgres."
    )
    @Builder.Default
    private final Property<Integer> maxBufferSize = Property.ofValue(104857600);

    @Schema(
        title = "How often, in rows received, to count the buffered rows and bytes to check if a flush is necessary.",
        description = "There's a slight performance penalty to checking the buffered records count or bytesize, so this" +
            " controls how often this is polled in order to mitigate the penalty. This value is usually not necessary" +
            " to set as the default is dynamically adjusted to check reasonably often. \n\n" +
            "Default is 5000, or 1/40th `maxBatchRows`"
    )
    private Property<Integer> batchDetectionThreshold;

    @Schema(
        title = "Whether the Target should create column indexes on the important columns used during data loading.",
        description = "These indexes will make data loading slightly slower but the deduplication phase much faster. Defaults to on for better baseline performance."
    )
    @Builder.Default
    private final Property<Boolean> addUpsertIndexes = Property.ofValue(true);

    @Schema(
        title = "Raw SQL statement(s) to execute as soon as the connection to Postgres is opened by the target.",
        description = "Useful for setup like SET ROLE or other connection state that is important."
    )
    private Property<String> beforeRunSql;

    @Schema(
        title = "Raw SQL statement(s) to before closing the connection to Postgres."
    )
    private Property<String> afterRunSql;

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("postgres_username", runContext.render(this.username))
            .put("postgres_password", runContext.render(this.password).as(String.class).orElse(null))
            .put("postgres_host", runContext.render(this.host))
            .put("postgres_port", runContext.render(this.port).as(Integer.class).orElseThrow())
            .put("postgres_database", runContext.render(this.dbName).as(String.class).orElse(null))
            .put("postgres_schema", runContext.render(this.schema).as(String.class).orElseThrow())
            .put("postgres_sslmode", runContext.render(this.sslMode).as(String.class).orElseThrow())
            .put("invalid_records_detect", runContext.render(this.invalidRecordsDetect).as(Boolean.class).orElseThrow())
            .put("invalid_records_threshold", runContext.render(this.invalidRecordsThreshold).as(Integer.class).orElseThrow())
            .put("disable_collection", true)
            .put("logging_level", runContext.render(this.loggingLevel).as(String.class).orElseThrow())
            .put("persist_empty_tables", runContext.render(this.persistEmptyTables).as(Boolean.class).orElseThrow())
            .put("max_batch_rows", runContext.render(this.maxBatchRows).as(Integer.class).orElseThrow())
            .put("max_buffer_size", runContext.render(this.maxBufferSize).as(Integer.class).orElseThrow())
            .put("state_support", true)
            .put("add_upsert_indexes", runContext.render(this.addUpsertIndexes).as(Boolean.class).orElseThrow());

        if (this.batchDetectionThreshold != null) {
            builder.put("batch_detection_threshold", runContext.render(this.batchDetectionThreshold).as(Integer.class).orElseThrow());
        }

        if (this.beforeRunSql != null) {
            builder.put("before_run_sql", runContext.render(this.beforeRunSql).as(String.class).orElseThrow());
        }

        if (this.afterRunSql != null) {
            builder.put("after_run_sql", runContext.render(this.afterRunSql).as(String.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("singer-target-postgres"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("target-postgres");
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.runTarget(runContext);
    }
}
