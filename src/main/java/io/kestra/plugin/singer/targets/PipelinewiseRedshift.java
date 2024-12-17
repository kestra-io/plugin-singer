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
    title = "A Singer target loads data into a Redshift database.",
    description = "Full documentation can be found [here](https://github.com/transferwise/pipelinewise-target-redshift)"
)
public class PipelinewiseRedshift extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
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
        title = "The database port."
    )
    private Property<Integer> port;

    @NotEmpty
    @NotNull
    @Schema(
        title = "The S3 bucket name."
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
        title = "S3 Access Key ID.",
        description = "Used for S3 and Redshift copy operations."
    )
    private Property<String> accessKeyId;

    @Schema(
        title = "S3 Secret Access Key.",
        description = "Used for S3 and Redshift copy operations."
    )
    private Property<String> secretAccessKey;

    @Schema(
        title = "AWS S3 Session Token.",
        description = "S3 AWS STS token for temporary credentials."
    )
    private Property<String> sessionToken;

    @Schema(
        title = "AWS Redshift COPY role ARN.",
        description = "AWS Role ARN to be used for the Redshift COPY operation. " +
            "Used instead of the given AWS keys for the COPY operation if provided - " +
            "the keys are still used for other S3 operations."
    )
    private Property<String> redshiftCopyRoleArn;

    @Schema(
        title = "AWS S3 ACL.",
        description = "S3 Object ACL."
    )
    private Property<String> s3Acl;

    @Schema(
        title = "S3 Key Prefix.",
        description = "A static prefix before the generated S3 key names. " +
            "Using prefixes you can upload files into specific directories in the S3 bucket. Default(None)."
    )
    private Property<String> s3KeyPrefix;

    @Schema(
        title = "COPY options.",
        description = "Parameters to use in the COPY command when loading data to Redshift. " +
            "Some basic file formatting parameters are fixed values and not recommended overriding them by custom values. " +
            "They are like: `CSV GZIP DELIMITER ',' REMOVEQUOTES ESCAPE`."
    )
    private Property<String> copyOptions;

    @Schema(
        title = "Maximum number of rows in each batch.",
        description = "At the end of each batch, the rows in the batch are loaded into Redshift."
    )
    @Builder.Default
    private final Property<Integer> batchSizeRows = Property.of(100000);

    @Schema(
        title = "Flush and load every stream into Redshift when one batch is full.",
        description = "Warning: This may trigger the COPY command to use files with low number of records.."
    )
    @Builder.Default
    private final Property<Boolean> flushAllStreams = Property.of(false);

    @Schema(
        title = "The number of threads used to flush tables.",
        description = "0 will create a thread for each stream, up to parallelism_max. -1 will create a thread for " +
            "each CPU core. Any other positive number will create that number of threads, up to parallelism_max."
    )
    @Builder.Default
    private final Property<Integer> parallelism = Property.of(0);

    @Schema(
        title = "Max number of parallel threads to use when flushing tables."
    )
    @Builder.Default
    private final Property<Integer> maxParallelism = Property.of(16);

    @Schema(
        title = "Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created tables to a specific list of users or groups.",
        description = "If `schemaMapping` is not defined then every stream sent by the tap is granted accordingly."
    )
    private Property<String> defaultTargetSchemaSelectPermissions;

    @Schema(
        title = "Schema mapping.",
        description = "Useful if you want to load multiple streams from one tap to multiple Redshift schemas. " +
            "If the tap sends the stream_id in <schema_name>-<table_name> format then this option overwrites the " +
            "`default_target_schema` value. Note, that using schema_mapping you can overwrite the " +
            "`default_target_schema_select_permissions` value to grant SELECT permissions to different groups per " +
            "schemas or optionally you can create indices automatically for the replicated tables."
    )
    private Property<String> schema_mapping;

    @Schema(
        title = "Disable table cache.",
        description = "By default the connector caches the available table structures in Redshift at startup. " +
            "In this way it doesn't need to run additional queries when ingesting data to check if altering the target " +
            "tables is required. With disable_table_cache option you can turn off this caching. You will always " +
            "see the most recent table structures but will cause an extra query runtime."
    )
    @Builder.Default
    private final Property<Boolean> disableTableCache = Property.of(false);

    @Schema(
        title = "Add metadata columns.",
        description = "Metadata columns add extra row level information about data ingestions, (i.e. when was the " +
            "row read in source, when was inserted or deleted in redshift etc.) Metadata columns are creating " +
            "automatically by adding extra columns to the tables with a column prefix _SDC_. The metadata columns " +
            "are documented at [here](https://transferwise.github.io/pipelinewise/index.html). " +
            "Enabling metadata columns will flag the deleted rows by setting the _SDC_DELETED_AT metadata column. " +
            "Without the `addMetadataColumns` option the deleted rows from singer taps will not be recongisable in Redshift."
    )
    @Builder.Default
    private final Property<Boolean> addMetadataColumns = Property.of(false);

    @Schema(
        title = "Delete rows on Redshift.",
        description = "When `hardDelete` option is true then DELETE SQL commands will be performed in Redshift to delete " +
            "rows in tables. It's achieved by continuously checking the _SDC_DELETED_AT metadata column sent by the " +
            "singer tap. Due to deleting rows requires metadata columns, `hardDelete` option automatically enables " +
            "the `addMetadataColumns` option as well."
    )
    @Builder.Default
    private final Property<Boolean> hardDelete = Property.of(false);

    @Schema(
        title = "Object type RECORD items from taps can be transformed to flattened columns by creating columns automatically.",
        description = "When `hardDelete` option is true then DELETE SQL commands will be performed in Redshift to delete " +
            "rows in tables. It's achieved by continuously checking the _SDC_DELETED_AT metadata column sent by the " +
            "singer tap. Due to deleting rows requires metadata columns, `hardDelete` option automatically " +
            "enables the `addMetadataColumns` option as well.."
    )
    @Builder.Default
    private final Property<Integer> dataFlatteningMaxLevel = Property.of(0);

    @Schema(
        title = "Log based and Incremental replications on tables with no Primary Key cause duplicates when merging UPDATE events.",
        description = "When set to true, stop loading data if no Primary Key is defined.."
    )
    @Builder.Default
    private final Property<Boolean> primaryKeyRequired = Property.of(true);

    @Schema(
        title = "Validate every single record message to the corresponding JSON schema.",
        description = "This option is disabled by default and invalid RECORD messages will fail only at load time by " +
            "Redshift. Enabling this option will detect invalid records earlier but could cause performance degradation.."
    )
    @Builder.Default
    private final Property<Boolean> validateRecords = Property.of(false);

    @Schema(
        title = "Do not update existing records when Primary Key is defined. ",
        description = "Useful to improve performance when records are immutable, e.g. events."
    )
    @Builder.Default
    private final Property<Boolean> skipUpdates = Property.of(false);

    @Schema(
        title = "The compression method to use when writing files to S3 and running Redshift COPY."
    )
    @Builder.Default
    private final Property<Compression> compression = Property.of(Compression.bzip2);

    @Schema(
        title = "number of slices to split files into prior to running COPY on Redshift.",
        description = "This should be set to the number of Redshift slices. The number of slices per node depends on " +
            "the node size of the cluster - run SELECT COUNT(DISTINCT slice) slices FROM stv_slices to calculate this."
    )
    @Builder.Default
    private final Property<Integer> slices = Property.of(1);

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("user", runContext.render(this.username))
            .put("password", runContext.render(this.password).as(String.class).orElse(null))
            .put("host", runContext.render(this.host))
            .put("port", runContext.render(this.port).as(Integer.class).orElseThrow())
            .put("dbname", runContext.render(this.dbName).as(String.class).orElseThrow())
            .put("s3_bucket", runContext.render(this.s3Bucket))
            .put("default_target_schema", runContext.render(this.defaultTargetSchema))
            .put("batch_size_rows", runContext.render(this.batchSizeRows).as(Integer.class).orElseThrow())
            .put("flush_all_streams", runContext.render(this.flushAllStreams).as(Boolean.class).orElseThrow())
            .put("parallelism", runContext.render(this.parallelism).as(Integer.class).orElseThrow())
            .put("max_parallelism", runContext.render(this.maxParallelism).as(Integer.class).orElseThrow())
            .put("disable_table_cache", runContext.render(this.disableTableCache).as(Boolean.class).orElseThrow())
            .put("addMetadataColumns", runContext.render(this.addMetadataColumns).as(Boolean.class).orElseThrow())
            .put("hardDelete", runContext.render(this.hardDelete).as(Boolean.class).orElseThrow())
            .put("data_flattening_max_level", runContext.render(this.dataFlatteningMaxLevel).as(Integer.class).orElseThrow())
            .put("primary_key_required", runContext.render(this.primaryKeyRequired).as(Boolean.class).orElseThrow())
            .put("validate_records", runContext.render(this.validateRecords).as(Boolean.class).orElseThrow())
            .put("skip_updates", runContext.render(this.skipUpdates).as(Boolean.class).orElseThrow())
            .put("compression", runContext.render(this.compression).as(Compression.class).orElseThrow())
            .put("slices", runContext.render(this.slices).as(Integer.class).orElseThrow());

        if (this.accessKeyId != null) {
            builder.put("aws_access_key_id", runContext.render(this.accessKeyId).as(String.class).orElseThrow());
        }

        if (this.secretAccessKey != null) {
            builder.put("aws_secret_access_key", runContext.render(this.secretAccessKey).as(String.class).orElseThrow());
        }

        if (this.sessionToken != null) {
            builder.put("aws_session_token", runContext.render(this.sessionToken).as(String.class).orElseThrow());
        }

        if (this.redshiftCopyRoleArn != null) {
            builder.put("aws_redshift_copy_role_arn", runContext.render(this.redshiftCopyRoleArn).as(String.class).orElseThrow());
        }

        if (this.s3Acl != null) {
            builder.put("s3_acl", runContext.render(this.s3Acl).as(String.class).orElseThrow());
        }

        if (this.s3KeyPrefix != null) {
            builder.put("s3_key_prefix", runContext.render(this.s3KeyPrefix).as(String.class).orElseThrow());
        }

        if (this.copyOptions != null) {
            builder.put("copy_options", runContext.render(this.copyOptions).as(String.class).orElseThrow());
        }

        if (this.defaultTargetSchemaSelectPermissions != null) {
            builder.put("default_target_schema_select_permissions", runContext.render(this.defaultTargetSchemaSelectPermissions).as(String.class).orElseThrow());
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("pipelinewise-target-redshift"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("target-redshift");
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
