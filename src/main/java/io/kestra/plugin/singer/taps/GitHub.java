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

import java.util.Arrays;
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
    title = "A Singer tap to fetch data from a GitHub api.",
    description = "Full documentation can be found [here](https://github.com/singer-io/tap-github)"
)
public class GitHub extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "a GitHub personnal access token",
        description = "Login to your GitHub account, " +
            "go to the [Personal Access Tokens](https://github.com/settings/tokens) settings page, " +
            "and generate a new token with at least the `repo` scope."
    )
    @PluginProperty(dynamic = true)
    private String accessToken;

    @NotNull
    @NotEmpty
    @Schema(
        title = "List of GitHub repositories.",
        description = "The repo path is relative to https://github.com/. \n" +
            "For example the path for [this repository](https://github.com/kestra-io/kestra) is `kestra-io/kestra`."
    )
    @PluginProperty(dynamic = true)
    private List<String> repositories;

    public List<Feature> features() {
        return Arrays.asList(
            Feature.PROPERTIES,
            Feature.DISCOVER,
            Feature.STATE
        );
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("access_token", runContext.render(this.accessToken))
            .put("repository", String.join(" ", runContext.render(this.repositories)));

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("tap-github");
    }

    @Override
    protected String command() {
        return "tap-github";
    }
}
