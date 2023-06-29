package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.statistics.DailyExecutionStatistics;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
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
public class PipelinewiseSnowflake extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
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

    @Schema(
        title = "Snowflake role to use. If not defined then the user's default role will be used"
    )
    @PluginProperty(dynamic = true)
    private String role;

    @Schema(
        title = "S3 Access Key Id. If not provided, `AWS_ACCESS_KEY_ID` environment variable or IAM role will be used "
    )
    @PluginProperty(dynamic = true)
    private String awsAccessKeyId;

    @Schema(
        title = "S3 Secret Access Key. If not provided, `AWS_SECRET_ACCESS_KEY` environment variable or IAM role will be used "
    )
    @PluginProperty(dynamic = true)
    private String awsSecretAccessKey;

    @Schema(
        title = "AWS Session token. If not provided, `AWS_SESSION_TOKEN` environment variable will be used "
    )
    @PluginProperty(dynamic = true)
    private String awsSessionToken;

    @Schema(
        title = "AWS profile name for profile based authentication. If not provided, `AWS_PROFILE` environment variable will be used. "
    )
    @PluginProperty(dynamic = true)
    private String awsProfile;

    @Schema(
        title = "S3 Bucket name. Required if to use S3 External stage. When this is defined then `stage` has to be defined as well. "
    )
    @PluginProperty(dynamic = true)
    private String s3Bucket;

    @Schema(
        title = "A static prefix before the generated S3 key names. Using prefixes you can upload files into specific directories in the S3 bucket. "
    )
    @PluginProperty(dynamic = true)
    private String s3KeyPrefix;

    @Schema(
        title = "The complete URL to use for the constructed client. This is allowing to use non-native s3 account. "
    )
    @PluginProperty(dynamic = true)
    private String s3EndpointUrl;

    @Schema(
        title = "Default region when creating new connections "
    )
    @PluginProperty(dynamic = true)
    private String s3RegionName;

    @Schema(
        title = "S3 ACL name to set on the uploaded files                                                   "
    )
    @PluginProperty(dynamic = true)
    private String s3Acl;

    @Schema(
        title = "Named external stage name created at pre-requirements section. Has to be a fully qualified name including the schema name. If not specified, table internal stage are used. When this is defined then `s3_bucket` has to be defined as well. "
    )
    @PluginProperty(dynamic = true)
    private String stage;

    @Schema(
        title = "Named file format name created at pre-requirements section. Has to be a fully qualified name including the schema name. "
    )
    @PluginProperty(dynamic = true)
    private String fileFormat;

    @Schema(
        title = "Maximum number of rows in each batch. At the end of each batch, the rows in the batch are loaded into Snowflake. "
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Integer batchSizeRows = 100000;

    @Schema(
        title = "Maximum time to wait for batch to reach `batch_size_rows`. "
    )
    @PluginProperty(dynamic = false)
    private Duration batchWaitLimit;

    @Schema(
        title = "Flush and load every stream into Snowflake when one batch is full. Warning: This may trigger the COPY command to use files with low number of records, and may cause performance problems. "
    )
    @PluginProperty(dynamic = true)
    private Boolean flushAllStreams = false;

    @Schema(
        title = "The number of threads used to flush tables. 0 will create a thread for each stream, up to parallelism_max. -1 will create a thread for each CPU core. Any other positive number will create that number of threads, up to parallelism_max. "
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Integer parallelism = 0;

    @Schema(
        title = "Max number of parallel threads to use when flushing tables. "
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Integer parallelismMax = 16;

    @Schema(
        title = "Name of the schema where the tables will be created, **without** database prefix. If `schema_mapping` is not defined then every stream sent by the tap is loaded into this schema.    "
    )
    @PluginProperty(dynamic = true)
    private String defaultTargetSchema;

    @Schema(
        title = "Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created tables to a specific role or a list of roles. If `schema_mapping` is not defined then every stream sent by the tap is granted accordingly.   "
    )
    @PluginProperty(dynamic = true)
    private String defaultTargetSchemaSelectPermission;

    @Schema(
        title = "Useful if you want to load multiple streams from one tap to multiple Snowflake schemas.<br><br>If the tap sends the `stream_id` in `<schema_name>-<table_name>` format then this option overwrites the `default_target_schema` value. Note, that using `schema_mapping` you can overwrite the `default_target_schema_select_permission` value to grant SELECT permissions to different groups per schemas or optionally you can create indices automatically for the replicated tables.<br><br> **Note**: This is an experimental feature and recommended to use via PipelineWise YAML files that will generate the object mapping in the right JSON format. For further info check a PipelineWise YAML Example"
    )
    @PluginProperty(dynamic = true)
    private String schemaMapping;

    @Schema(
        title = "By default the connector caches the available table structures in Snowflake at startup. In this way it doesn't need to run additional queries when ingesting data to check if altering the target tables is required. With `disable_table_cache` option you can turn off this caching. You will always see the most recent table structures but will cause an extra query runtime. "
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Boolean disableTableCache = false;

    @Schema(
        title = "When this is defined, Client-Side Encryption is enabled. The data in S3 will be encrypted, No third parties, including Amazon AWS and any ISPs, can see data in the clear. Snowflake COPY command will decrypt the data once it's in Snowflake. The master key must be 256-bit length and must be encoded as base64 string. "
    )
    @PluginProperty(dynamic = true)
    private String clientSideEncryptionMasterKey;

    @Schema(
        title = "Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in snowflake etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The column names are following the stitch naming conventions documented at https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recongisable in Snowflake. "
    )

    @Builder.Default
    @PluginProperty(dynamic = false)
    private Boolean addMetadataColumns = false;

    @Schema(
        title = "When `hardDelete` option is true then DELETE SQL commands will be performed in Snowflake to delete rows in tables. It's achieved by continuously checking the  `_SDC_DELETED_AT` metadata column sent by the singer tap. Due to deleting rows requires metadata columns, `hard_delete` option automatically enables the `add_metadata_columns` option as well. "
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean hardDelete = false;

    @Schema(
        title = "(Default: 0) Object type RECORD items from taps can be loaded into VARIANT columns as JSON (default) or we can flatten the schema by creating columns automatically.<br><br>When value is 0 (default) then flattening functionality is turned off. "
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Integer dataFlatteningMaxLevel = 0;

    @Schema(
        title = "Log based and Incremental replications on tables with no Primary Key cause duplicates when merging UPDATE events. When set to true, stop loading data if no Primary Key is defined. "
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean primaryKeyRequired = true;

    @Schema(
        title = "Validate every single record message to the corresponding JSON schema. This option is disabled by default and invalid RECORD messages will fail only at load time by Snowflake. Enabling this option will detect invalid records earlier but could cause performance degradation. "
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Boolean validateRecords = false;

    @Schema(
        title = "Generate uncompressed files when loading to Snowflake. Normally, by default GZIP compressed files are generated. "
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean noCompression = false;

    @Schema(
        title = "Optional string to tag executed queries in Snowflake. Replaces tokens `{{database}}`, `{{schema}}` and `{{table}}` with the appropriate values. The tags are displayed in the output of the Snowflake `QUERY_HISTORY`, `QUERY_HISTORY_BY_*` functions. "
    )
    @PluginProperty(dynamic = true)
    private String queryTag;

    @Schema(
        title = "When enabled, the files loaded to Snowflake will also be stored in `archive_load_files_s3_bucket` under the key `/{archive_load_files_s3_prefix}/{schema_name}/{table_name}/`. All archived files will have `tap`, `schema`, `table` and `archived-by` as S3 metadata keys. When incremental replication is used, the archived files will also have the following S3 metadata keys: `incremental-key`, `incremental-key-min` and `incremental-key-max`"
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean archiveLoadFiles = false;

    @Schema(
        title = "(Default: `archive`) When `archive_load_files` is enabled, the archived files will be placed in the archive S3 bucket under this prefix"
    )
    @PluginProperty(dynamic = true)
    private String archiveLoadFilesS3Prefix;

    @Schema(
        title = "(Default: Value of `s3_bucket`) When `archive_load_files` is enabled, the archived files will be placed in this bucket"
    )
    @PluginProperty(dynamic = true)
    private String archiveLoadFilesS3Bucket;

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("account", runContext.render(this.account))
            .put("dbname", runContext.render(this.database))
            .put("user", runContext.render(this.username))
            .put("password", runContext.render(this.password))
            .put("warehouse", runContext.render(this.warehouse));

        if (this.role != null) {
            builder.put("role", runContext.render(this.role));
        }

        if (this.awsAccessKeyId != null) {
            builder.put("aws_access_key_id", runContext.render(this.awsAccessKeyId));
        }

        if (this.awsSecretAccessKey != null) {
            builder.put("aws_secret_access_key", runContext.render(this.awsSecretAccessKey));
        }

        if (this.awsSessionToken != null) {
            builder.put("aws_session_token", runContext.render(this.awsSessionToken));
        }

        if (this.awsProfile != null) {
            builder.put("aws_profile", runContext.render(this.awsProfile));
        }

        if (this.s3Bucket != null) {
            builder.put("s3_bucket", runContext.render(this.s3Bucket));
        }

        if (this.s3KeyPrefix != null) {
            builder.put("s3_key_prefix", runContext.render(this.s3KeyPrefix));
        }

        if (this.s3EndpointUrl != null) {
            builder.put("s3_endpoint_url", runContext.render(this.s3EndpointUrl));
        }

        if (this.s3RegionName != null) {
            builder.put("s3_region_name", runContext.render(this.s3RegionName));
        }

        if (this.s3Acl != null) {
            builder.put("s3_acl", runContext.render(this.s3Acl));
        }

        if (this.stage != null) {
            builder.put("stage", runContext.render(this.stage));
        }

        if (this.fileFormat != null) {
            builder.put("file_format", runContext.render(this.fileFormat));
        }

        if (this.batchSizeRows != null) {
            builder.put("batch_size_rows", this.batchSizeRows);
        }

        if (this.batchWaitLimit != null) {
            builder.put("batch_wait_limit_seconds", this.batchWaitLimit.toSeconds());
        }

        if (this.flushAllStreams != null) {
            builder.put("flush_all_streams", this.flushAllStreams.toString());
        }

        if (this.parallelism != null) {
            builder.put("parallelism", this.parallelism);
        }

        if (this.parallelismMax != null) {
            builder.put("parallelism_max", this.parallelismMax);
        }

        if (this.defaultTargetSchema != null) {
            builder.put("default_target_schema", runContext.render(this.defaultTargetSchema));
        }

        if (this.defaultTargetSchemaSelectPermission != null) {
            builder.put("default_target_schema_select_permission", runContext.render(this.defaultTargetSchemaSelectPermission));
        }

        if (this.schemaMapping != null) {
            builder.put("schema_mapping", runContext.render(this.schemaMapping));
        }

        if (this.disableTableCache != null) {
            builder.put("disable_table_cache", this.disableTableCache.toString());
        }

        if (this.clientSideEncryptionMasterKey != null) {
            builder.put("client_side_encryption_master_key", runContext.render(this.clientSideEncryptionMasterKey));
        }

        if (this.addMetadataColumns != null) {
            builder.put("add_metadata_columns", this.addMetadataColumns.toString());
        }

        if (this.hardDelete != null) {
            builder.put("hard_delete", this.hardDelete.toString());
        }

        if (this.dataFlatteningMaxLevel != null) {
            builder.put("data_flattening_max_level", this.dataFlatteningMaxLevel);
        }

        if (this.primaryKeyRequired != null) {
            builder.put("primary_key_required", this.primaryKeyRequired.toString());
        }

        if (this.validateRecords != null) {
            builder.put("validate_records", this.validateRecords.toString());
        }

        if (this.noCompression != null) {
            builder.put("no_compression", this.noCompression.toString());
        }

        if (this.queryTag != null) {
            builder.put("query_tag", runContext.render(this.queryTag));
        }

        if (this.archiveLoadFiles != null) {
            builder.put("archive_load_files", this.archiveLoadFiles.toString());
        }

        if (this.archiveLoadFilesS3Prefix != null) {
            builder.put("archive_load_files_s3_prefix", runContext.render(this.archiveLoadFilesS3Prefix));
        }

        if (this.archiveLoadFilesS3Bucket != null) {
            builder.put("archive_load_files_s3_bucket", runContext.render(this.archiveLoadFilesS3Bucket));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("pipelinewise-target-snowflake\n");
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
