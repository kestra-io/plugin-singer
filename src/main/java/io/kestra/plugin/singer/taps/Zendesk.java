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
    title = "A Singer tap to fetch data from a Zendesk account.",
    description = "Full documentation can be found [here](https://github.com/singer-io/tap-zendesk)"
)
public class Zendesk extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Zendesk Subdomain.",
        description = "See [Zendesk Documentation](https://support.zendesk.com/hc/en-us/articles/4409381383578-Where-can-I-find-my-Zendesk-subdomain)"
    )
    @PluginProperty(dynamic = true)
    private String subdomain;

    @Schema(
        title = "Zendesk email."
    )
    @PluginProperty(dynamic = true)
    private String email;

    @Schema(
        title = "Zendesk API token."
    )
    @PluginProperty(dynamic = true)
    private String apiToken;

    @Schema(
        title = "Zendesk access token.",
        description = "See [Zendesk Documentation](https://support.zendesk.com/hc/en-us/articles/203663836)"
    )
    @PluginProperty(dynamic = true)
    private String accessToken;

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
            .put("subdomain", runContext.render(this.subdomain))
            .put("start_date", runContext.render(this.startDate.toString()));

        if (this.email != null) {
            builder.put("email", runContext.render(this.email));
        }

        if (this.apiToken != null) {
            builder.put("api_token", runContext.render(this.apiToken));
        }

        if (this.accessToken != null) {
            builder.put("access_token", runContext.render(this.accessToken));
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("tap-zendesk");
    }

    @Override
    protected String command() {
        return "tap-zendesk";
    }
}
