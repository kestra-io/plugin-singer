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
    title = "A Singer target loads data into a Redshift database.",
    description = "Full documentation can be found [here](https://github.com/transferwise/pipelinewise-target-redshift)"
)
public class PipelinewiseRedshift extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
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

    @NotEmpty
    @NotNull
    @Schema(
        title = "The S3 bucket name"
    )
    @PluginProperty(dynamic = true)
    private String s3Bucket;

    @NotEmpty
    @NotNull
    @Schema(
        title = "Name of the schema where the tables will be created.",
        description = "If schema_mapping is not defined then every stream sent by the tap is loaded into this schema."
    )
    @PluginProperty(dynamic = true)
    private String defaultTargetSchema;

    @Schema(
        title = "S3 Access Key Id.",
        description = "Used for S3 and Redshift copy operations."
    )
    @PluginProperty(dynamic = true)
    private String accessKeyId;

    @Schema(
        title = "S3 Secret Access Key.",
        description = "Used for S3 and Redshift copy operations."
    )
    @PluginProperty(dynamic = true)
    private String secretAccessKey;

    @Schema(
        title = "AWS S3 Session Token.",
        description = "S3 AWS STS token for temporary credentials."
    )
    @PluginProperty(dynamic = true)
    private String sessionToken;

    @Schema(
        title = "AWS Redshift COPY role ARN.",
        description = "AWS Role ARN to be used for the Redshift COPY operation. " +
            "Used instead of the given AWS keys for the COPY operation if provided - " +
            "the keys are still used for other S3 operations."
    )
    @PluginProperty(dynamic = true)
    private String redshiftCopyRoleArn;

    @Schema(
        title = "AWS S3 ACL.",
        description = "S3 Object ACL."
    )
    @PluginProperty(dynamic = true)
    private String s3Acl;

    @Schema(
        title = "S3 Key Prefix.",
        description = "A static prefix before the generated S3 key names. " +
            "Using prefixes you can upload files into specific directories in the S3 bucket. Default(None)."
    )
    @PluginProperty(dynamic = true)
    private String s3KeyPrefix;

    @Schema(
        title = "COPY options.",
        description = "Parameters to use in the COPY command when loading data to Redshift. " +
            "Some basic file formatting parameters are fixed values and not recommended overriding them by custom values. " +
            "They are like: `CSV GZIP DELIMITER ',' REMOVEQUOTES ESCAPE`."
    )
    @PluginProperty(dynamic = true)
    private String copyOptions;

    @Schema(
        title = "Maximum number of rows in each batch.",
        description = "At the end of each batch, the rows in the batch are loaded into Redshift."
    )
    @PluginProperty
    @Builder.Default
    private final Integer batchSizeRows = 100000;

    @Schema(
        title = "Flush and load every stream into Redshift when one batch is full.",
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
        title = "Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created tables to a specific list of users or groups.",
        description = "If `schemaMapping` is not defined then every stream sent by the tap is granted accordingly."
    )
    @PluginProperty(dynamic = true)
    private String defaultTargetSchemaSelectPermissions;

    @Schema(
        title = "Schema mapping.",
        description = "Useful if you want to load multiple streams from one tap to multiple Redshift schemas. " +
            "If the tap sends the stream_id in <schema_name>-<table_name> format then this option overwrites the " +
            "`default_target_schema` value. Note, that using schema_mapping you can overwrite the " +
            "`default_target_schema_select_permissions` value to grant SELECT permissions to different groups per " +
            "schemas or optionally you can create indices automatically for the replicated tables."
    )
    @PluginProperty
    private String schema_mapping;

    @Schema(
        title = "Disable table cache.",
        description = "By default the connector caches the available table structures in Redshift at startup. " +
            "In this way it doesn't need to run additional queries when ingesting data to check if altering the target " +
            "tables is required. With disable_table_cache option you can turn off this caching. You will always " +
            "see the most recent table structures but will cause an extra query runtime."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean disableTableCache = false;

    @Schema(
        title = "Add metadata columns.",
        description = "Metadata columns add extra row level information about data ingestions, (i.e. when was the " +
            "row read in source, when was inserted or deleted in redshift etc.) Metadata columns are creating " +
            "automatically by adding extra columns to the tables with a column prefix _SDC_. The metadata columns " +
            "are documented at [here](https://transferwise.github.io/pipelinewise/index.html). " +
            "Enabling metadata columns will flag the deleted rows by setting the _SDC_DELETED_AT metadata column. " +
            "Without the `addMetadataColumns` option the deleted rows from singer taps will not be recongisable in Redshift."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean addMetadataColumns = false;

    @Schema(
        title = "Delete rows on Redshift.",
        description = "When `hardDelete` option is true then DELETE SQL commands will be performed in Redshift to delete " +
            "rows in tables. It's achieved by continuously checking the _SDC_DELETED_AT metadata column sent by the " +
            "singer tap. Due to deleting rows requires metadata columns, `hardDelete` option automatically enables " +
            "the `addMetadataColumns` option as well."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean hardDelete = false;

    @Schema(
        title = "Object type RECORD items from taps can be transformed to flattened columns by creating columns automatically.",
        description = "When `hardDelete` option is true then DELETE SQL commands will be performed in Redshift to delete " +
            "rows in tables. It's achieved by continuously checking the _SDC_DELETED_AT metadata column sent by the " +
            "singer tap. Due to deleting rows requires metadata columns, `hardDelete` option automatically " +
            "enables the `addMetadataColumns` option as well.."
    )
    @PluginProperty
    @Builder.Default
    private final Integer dataFlatteningMaxLevel = 0;

    @Schema(
        title = "Log based and Incremental replications on tables with no Primary Key cause duplicates when merging UPDATE events.",
        description = "When set to true, stop loading data if no Primary Key is defined.."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean primaryKeyRequired = true;

    @Schema(
        title = "Validate every single record message to the corresponding JSON schema.",
        description = "This option is disabled by default and invalid RECORD messages will fail only at load time by " +
            "Redshift. Enabling this option will detect invalid records earlier but could cause performance degradation.."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean validateRecords = false;

    @Schema(
        title = "Do not update existing records when Primary Key is defined. ",
        description = "Useful to improve performance when records are immutable, e.g. events."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean skipUpdates = false;

    @Schema(
        title = "The compression method to use when writing files to S3 and running Redshift COPY."
    )
    @PluginProperty
    @Builder.Default
    private final Compression compression = Compression.bzip2;

    @Schema(
        title = "number of slices to split files into prior to running COPY on Redshift.",
        description = "This should be set to the number of Redshift slices. The number of slices per node depends on " +
            "the node size of the cluster - run SELECT COUNT(DISTINCT slice) slices FROM stv_slices to calculate this."
    )
    @PluginProperty
    @Builder.Default
    private final Integer slices = 1;

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("user", runContext.render(this.username))
            .put("password", runContext.render(this.password))
            .put("host", runContext.render(this.host))
            .put("port", this.port)
            .put("dbname", runContext.render(this.dbName))
            .put("s3_bucket", runContext.render(this.s3Bucket))
            .put("default_target_schema", runContext.render(this.defaultTargetSchema))
            .put("batch_size_rows", this.batchSizeRows)
            .put("flush_all_streams", this.flushAllStreams)
            .put("parallelism", this.parallelism)
            .put("max_parallelism", this.maxParallelism)
            .put("disable_table_cache", this.disableTableCache)
            .put("addMetadataColumns", this.addMetadataColumns)
            .put("hardDelete", this.hardDelete)
            .put("data_flattening_max_level", this.dataFlatteningMaxLevel)
            .put("primary_key_required", this.primaryKeyRequired)
            .put("validate_records", this.validateRecords)
            .put("skip_updates", this.skipUpdates)
            .put("compression", this.compression)
            .put("slices", this.slices);

        if (this.accessKeyId != null) {
            builder.put("aws_access_key_id", runContext.render(this.accessKeyId));
        }

        if (this.secretAccessKey != null) {
            builder.put("aws_secret_access_key", runContext.render(this.secretAccessKey));
        }

        if (this.sessionToken != null) {
            builder.put("aws_session_token", runContext.render(this.sessionToken));
        }

        if (this.redshiftCopyRoleArn != null) {
            builder.put("aws_redshift_copy_role_arn", runContext.render(this.redshiftCopyRoleArn));
        }

        if (this.s3Acl != null) {
            builder.put("s3_acl", runContext.render(this.s3Acl));
        }

        if (this.s3KeyPrefix != null) {
            builder.put("s3_key_prefix", runContext.render(this.s3KeyPrefix));
        }

        if (this.copyOptions != null) {
            builder.put("copy_options", runContext.render(this.copyOptions));
        }

        if (this.defaultTargetSchemaSelectPermissions != null) {
            builder.put("default_target_schema_select_permissions", runContext.render(this.defaultTargetSchemaSelectPermissions));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("pipelinewise-target-redshift");
    }

    @Override
    protected String command() {
        return "target-redshift";
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.runTarget(runContext);
    }

    public enum Compression {
        gzip,
        bzip2
    }
}
