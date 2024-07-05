package io.kestra.plugin.singer.taps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
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
    title = "A Singer tap to fetch data from a GitHub API.",
    description = "Full documentation can be found [here](https://github.com/singer-io/tap-github)"
)
public class GitHub extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "A GitHub personal access token.",
        description = "Login to your GitHub account, " +
            "go to the [Personal Access Tokens](https://github.com/settings/tokens) settings page, " +
            "and generate a new token with at least the `repo` scope."
    )
    @PluginProperty(dynamic = true)
    private String accessToken;

    @NotNull
    @Schema(
        title = "List of GitHub repositories.",
        description = "The repo path is relative to https://github.com/. \n" +
            "For example the path for [this repository](https://github.com/kestra-io/kestra) is `kestra-io/kestra`."
    )
    @PluginProperty(dynamic = true)
    private Object repositories;

    @NotNull
    @Schema(
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private LocalDate startDate;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Timeout for each request on github API."
    )
    @PluginProperty
    @Builder.Default
    private Integer requestTimeout = 300;

    public List<Feature> features() {
        return Arrays.asList(
            Feature.PROPERTIES,
            Feature.DISCOVER,
            Feature.STATE
        );
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> repositories;
        if (this.repositories instanceof List) {
            //noinspection unchecked
            repositories = (List<String>) this.repositories;
        } else if (this.repositories instanceof String) {
            final TypeReference<List<String>> reference = new TypeReference<>() {};

            try {
                repositories = JacksonMapper.ofJson(false).readValue(
                    runContext.render((String) this.repositories),
                    reference
                );
            } catch (JsonProcessingException e) {
                throw new IllegalVariableEvaluationException(e);
            }
        } else {
            throw new IllegalVariableEvaluationException("Invalid `files` properties with type '" + this.repositories.getClass() + "'");
        }


        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("access_token", runContext.render(this.accessToken))
            .put("repository", String.join(" ", repositories))
            .put("start_date", runContext.render(this.startDate.toString()))
            .put("request_timeout", this.requestTimeout);

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
