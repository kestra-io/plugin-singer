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
    title = "Fetch data from a Marketo account with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://gitlab.com/meltano/tap-marketo.git)."
)
public class Marketo extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Endpoint URL.",
        description = "The base URL contains the account id (a.k.a. Munchkin id) and is therefore unique for each " +
            "Marketo subscription. Your base URL is found by logging into Marketo and navigating to the " +
            "Admin > Integration > Web Services menu. " +
            "It is labeled as “Endpoint:” underneath the “REST API” section as shown in the following screenshots."
    )
    @PluginProperty(dynamic = true)
    private String endpoint;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Identity.",
        description = "Identity is found directly below the endpoint entry." +
            "https://developers.marketo.com/rest-api/base-url/"
    )
    @PluginProperty(dynamic = true)
    private String identity;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Marketo client ID."
    )
    @PluginProperty(dynamic = true)
    private String clientId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Marketo client secret."
    )
    @PluginProperty(dynamic = true)
    private String clientSecret;

    @NotNull
    @Schema(
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private LocalDate startDate;

    public List<Feature> features() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("endpoint", runContext.render(this.endpoint))
            .put("identity", runContext.render(this.identity))
            .put("client_id", runContext.render(this.clientId))
            .put("client_secret", runContext.render(this.clientSecret))
            .put("start_date", runContext.render(this.startDate.toString()));

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("git+https://gitlab.com/meltano/tap-marketo.git"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-marketo");
    }
}
