package io.kestra.plugin.singer.targets;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
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
    title = "A Singer target loads data into a BigQuery.",
    description = "Full documentation can be found [here](https://github.com/adswerve/target-bigquery)"
)
public class AdswerveBigQuery extends AbstractPythonTarget implements RunnableTask<AbstractPythonTarget.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "The BigQuery project."
    )
    @PluginProperty(dynamic = true)
    private String projectId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The BigQuery dataset."
    )
    @PluginProperty(dynamic = true)
    private String datasetId;

    @Schema(
        title = "The Dataset location."
    )
    @PluginProperty(dynamic = true)
    private String location;

    @NotNull
    @Schema(
        title = "Validate every single record message to the corresponding JSON schema.",
        description = "This option is disabled by default and invalid RECORD messages will fail only at load time by " +
            "Postgres. Enabling this option will detect invalid records earlier but could cause performance degradation.."
    )
    @PluginProperty
    private final Boolean validateRecords = false;

    @Schema(
        title = "Add singer Metadata columns.",
        description = "Add `_time_extracted` and `_time_loaded` metadata columns."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean addMetadataColumns = false;

    @Schema(
        title = "The replication method, `append` or `truncate`."
    )
    @PluginProperty
    @Builder.Default
    private final ReplicationMethod replicationMethod = ReplicationMethod.append;

    @Schema(
        title = "Add prefix to table name."
    )
    @PluginProperty(dynamic = true)
    private String tablePrefix;

    @Schema(
        title = "Add suffix to table name."
    )
    @PluginProperty(dynamic = true)
    private String tableSuffix;

    @Schema(
        title = "Maximum cache size in MB."
    )
    @PluginProperty
    @Builder.Default
    private final Integer maxCache = 50;

    @Schema(
        title = "The JSON service account key as string."
    )
    @PluginProperty(dynamic = true)
    protected String serviceAccount;

    @Schema(
        title = "Enable control state flush.",
        description = "default: merges multiple state messages from the tap into the state file, if true : uses the last state message as the state file."
    )
    @PluginProperty
    @Builder.Default
    protected Boolean mergeStateMessages = false;

    @Schema(
        title = "Table configs."
    )
    @PluginProperty
    protected Map<String, Object> tableConfigs;

    @SneakyThrows
    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("project_id", runContext.render(this.projectId))
            .put("dataset_id", runContext.render(this.datasetId))
            .put("validate_records", this.validateRecords)
            .put("add_metadata_columns", this.addMetadataColumns)
            .put("replication_method", this.replicationMethod)
            .put("max_cache", this.maxCache);

        if (this.location != null) {
            builder.put("location", runContext.render(this.location));
        }

        if (this.tablePrefix != null) {
            builder.put("table_prefix", runContext.render(this.tablePrefix));
        }

        if (this.tableSuffix != null) {
            builder.put("table_suffix", runContext.render(this.tableSuffix));
        }

        if (this.mergeStateMessages) {
            builder.put("merge_state_messages", "0");
        }

        if (this.tableConfigs != null) {
            this.writeSingerFiles("table-config.json", runContext.render(this.tableConfigs));
            builder.put("table_config", workingDirectory.toAbsolutePath() + "/table-config.json");
        }

        return builder.build();
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    protected Map<String, String> environmentVariables(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        HashMap<String, String> env = new HashMap<>(super.environmentVariables(runContext));

        if (this.serviceAccount != null) {
            this.writeSingerFiles("google-credentials.json", runContext.render(this.serviceAccount));
            env.put("GOOGLE_APPLICATION_CREDENTIALS", workingDirectory.toAbsolutePath() + "/google-credentials.json");
        }

        return env;
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("git+https://github.com/kestra-io/target-bigquery.git@fix");
    }

    @Override
    protected String command() {
        return "target-bigquery";
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.runTarget(runContext);
    }

    public enum ReplicationMethod {
        append,
        truncate
    }
}
