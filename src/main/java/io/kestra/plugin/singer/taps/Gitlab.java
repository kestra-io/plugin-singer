package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    title = "Fetch data from a GitLab account with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://gitlab.com/meltano/tap-gitlab.git)."
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
    private Property<List<String>> groups;

    @Schema(
        title = "`namespace/project` paths of projects to extract data from.",
        description = "Leave empty and provide a group name to extract data from all group projects."
    )
    private Property<List<String>> projects;

    @NotNull
    @Schema(
        title = "Enable to pull in extra data (like Epics, Epic Issues and other entities) only available to GitLab Ultimate and GitLab.com Gold accounts."
    )
    private final Property<Boolean> ultimateLicense = Property.of(false);

    @NotNull
    @Schema(
        title = "For each Merge Request, also fetch the MR's commits and create the join table `merge_request_commits` with the Merge Request and related Commit IDs.",
        description = "This can slow down extraction considerably because of the many API calls required."
    )
    private final Property<Boolean> fetchMergeRequestCommits = Property.of(false);

    @NotNull
    @Schema(
        title = "For every Pipeline, also fetch extended details of each of these pipelines.",
        description = "This can slow down extraction considerably because of the many API calls required."
    )
    private final Property<Boolean> fetchPipelinesExtended = Property.of(false);

    @NotNull
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
            .put("ultimate_license", runContext.render(this.ultimateLicense).as(Boolean.class).orElseThrow())
            .put("fetch_merge_request_commits", runContext.render(this.fetchMergeRequestCommits).as(Boolean.class).orElseThrow())
            .put("fetch_pipelines_extended", runContext.render(this.fetchPipelinesExtended).as(Boolean.class).orElseThrow())
            .put("start_date", runContext.render(this.startDate.toString()));

        var renderedGroups = runContext.render(this.groups).asList(String.class);
        if (!renderedGroups.isEmpty()) {
            builder.put("groups", String.join(" ", renderedGroups));
        }

        var renderedProjects = runContext.render(projects).asList(String.class);
        if (!renderedProjects.isEmpty()) {
            builder.put("projects", String.join(" ", renderedProjects));
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("git+https://gitlab.com/meltano/tap-gitlab.git"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("tap-gitlab");
    }
}
