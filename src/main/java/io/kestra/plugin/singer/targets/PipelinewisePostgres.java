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
    description = "Full documentation can be found [here](https://github.com/transferwise/pipelinewise-target-postgres)"
)
public class PipelinewisePostgres extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
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
    @PluginProperty
    private Integer port;

    @Schema(
        title = "Maximum number of rows in each batch.",
        description = "At the end of each batch, the rows in the batch are loaded into Postgres."
    )
    @PluginProperty
    @Builder.Default
    private final Integer batchSizeRows = 100000;

    @Schema(
        title = "Flush and load every stream into Postgres when one batch is full.",
        description = "Warning: This may trigger the COPY command to use files with low number of records.."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean flushAllStreams = false;

    @Schema(
        title = "The number of threads used to flush tables.",
        description = "0 will create a thread for each stream, up to parallelism_max. -1 will create a thread for " +
            "each CPU core. Any other positive number will create that number of threads, up to parallelism_max."
    )
    @PluginProperty
    @Builder.Default
    private final Integer parallelism = 0;

    @Schema(
        title = "Max number of parallel threads to use when flushing tables."
    )
    @PluginProperty
    @Builder.Default
    private final Integer maxParallelism = 16;

    @Schema(
        title = "Add singer Metadata columns.",
        description = "Metadata columns add extra row level information about data ingestions, " +
            "(i.e. when was the row read in source, when was inserted or deleted in postgres etc.) " +
            "Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. " +
            "The column names are following the [stitch naming conventions](https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns). " +
            "Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. " +
            "Without the `add_metadata_columns` option the deleted rows from singer taps will not be recognisable in Postgres."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean addMetadataColumns = false;

    @Schema(
        title = "Delete rows on Postgres.",
        description = "When hard_delete option is true then DELETE SQL commands will be performed in Postgres to " +
            "delete rows in tables. It's achieved by continuously checking the `_SDC_DELETED_AT` metadata column sent " +
            "by the singer tap. Due to deleting rows requires metadata columns, hard_delete option automatically " +
            "enables the add_metadata_columns option as well."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean hardDelete = false;

    @Schema(
        title = "Object type RECORD items from taps can be transformed to flattened columns by creating columns automatically.",
        description = "When value is 0 (default) then flattening functionality is turned off."
    )
    @PluginProperty
    @Builder.Default
    private final Integer dataFlatteningMaxLevel = 0;

    @Schema(
        title = "Log based and Incremental replications on tables with no Primary Key cause duplicates when merging UPDATE events.",
        description = "When set to true, stop loading data if no Primary Key is defined."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean primaryKeyRequired = true;

    @Schema(
        title = "Validate every single record message to the corresponding JSON schema.",
        description = "This option is disabled by default and invalid RECORD messages will fail only at load time by " +
            "Postgres. Enabling this option will detect invalid records earlier but could cause performance degradation.."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean validateRecords = false;

    @Schema(
        title = "Name of the schema where the tables will be created.",
        description = "If `schemaMapping` is not defined then every stream sent by the tap is loaded into this schema."
    )
    @PluginProperty(dynamic = true)
    private String defaultTargetSchema;

    @Schema(
        title = "Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created."
    )
    @PluginProperty(dynamic = true)
    private String defaultTargetSchemaSelectPermission;

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("user", runContext.render(this.username))
            .put("password", runContext.render(this.password))
            .put("host", runContext.render(this.host))
            .put("port", this.port)
            .put("dbname", runContext.render(this.dbName))
            .put("batch_size_rows", this.batchSizeRows)
            .put("flush_all_streams", this.flushAllStreams)
            .put("parallelism", this.parallelism)
            .put("max_parallelism", this.maxParallelism)
            .put("add_metadata_columns", this.addMetadataColumns)
            .put("hard_delete", this.hardDelete)
            .put("data_flattening_max_level", this.dataFlatteningMaxLevel)
            .put("primary_key_required", this.primaryKeyRequired)
            .put("validate_records", this.validateRecords);

        if (this.defaultTargetSchema != null) {
            builder.put("default_target_schema", runContext.render(this.defaultTargetSchema));
        }

        if (this.defaultTargetSchema != null) {
            builder.put("default_target_schema_select_permission", runContext.render(this.defaultTargetSchemaSelectPermission));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("pipelinewise-target-postgres");
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
