package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.LocalDate;
import java.util.Arrays;
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
    title = "A Singer tap to fetch data from a Gitlab account.",
    description = "Full documentation can be found [here](https://gitlab.com/meltano/tap-gitlab.git)"
)
public class Gitlab extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "GitLab API/instance URL.",
        description = "When an API path is omitted, `/api/v4/` is assumed."
    )
    @PluginProperty(dynamic = true)
    private final String apiUrl = "https://gitlab.com";

    @NotNull
    @NotEmpty
    @Schema(
        title = "GitLab personal access token or other API token."
    )
    @PluginProperty(dynamic = true)
    private String private_token;

    @Schema(
        title = "Names of groups to extract data from.",
        description = "Leave empty and provide a project name if you'd like to pull data from a project in a personal user namespace."
    )
    @PluginProperty(dynamic = true)
    private List<String> groups;

    @Schema(
        title = "`namespace/project` paths of projects to extract data from.",
        description = "Leave empty and provide a group name to extract data from all group projects."
    )
    @PluginProperty(dynamic = true)
    private List<String> projects;

    @NotNull
    @Schema(
        title = "Enable to pull in extra data (like Epics, Epic Issues and other entities) only available to GitLab Ultimate and GitLab.com Gold accounts."
    )
    @PluginProperty(dynamic = true)
    private final Boolean ultimateLicense = false;

    @NotNull
    @Schema(
        title = "For each Merge Request, also fetch the MR's commits and create the join table `merge_request_commits` with the Merge Request and related Commit IDs.",
        description = "This can slow down extraction considerably because of the many API calls required."
    )
    @PluginProperty(dynamic = true)
    private final Boolean fetchMergeRequestCommits = false;

    @NotNull
    @Schema(
        title = "For every Pipeline, also fetch extended details of each of these pipelines.",
        description = "This can slow down extraction considerably because of the many API calls required."
    )
    @PluginProperty(dynamic = true)
    private final Boolean fetchPipelinesExtended = false;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private LocalDate startDate;

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
            .put("api_url", runContext.render(this.apiUrl))
            .put("private_token", runContext.render(this.private_token))
            .put("ultimate_license", this.ultimateLicense)
            .put("fetch_merge_request_commits", this.fetchMergeRequestCommits)
            .put("fetch_pipelines_extended", this.fetchPipelinesExtended)
            .put("start_date", runContext.render(this.startDate.toString()));

        if (groups != null) {
            builder.put("groups", String.join(" ", runContext.render(this.groups)));
        }

        if (projects != null) {
            builder.put("projects", String.join(" ", runContext.render(this.projects)));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("git+https://gitlab.com/meltano/tap-gitlab.git");
    }

    @Override
    protected String command() {
        return "tap-gitlab";
    }
}
