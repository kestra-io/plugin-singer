package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
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
    title = "A Singer tap to fetch data from a Quickbooks account.",
    description = "Full documentation can be found [here](https://github.com/hotgluexyz/tap-quickbooks.git)"
)
public class Quickbooks extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @Schema(
        title = "Select by default any new fields discovered in Quickbooks objects."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean selectFieldsByDefault = true;

    @NotNull
    @Schema(
        title = "Select by default any new fields discovered in Quickbooks objects."
    )
    @PluginProperty
    @Builder.Default
    private final Boolean isSandbox = false;

    @NotNull
    @Schema(
        title = "Generate a STATE message every N records."
    )
    @PluginProperty
    @Builder.Default
    private final Integer stateMessageThreshold = 1000;

    @NotNull
    @Schema(
        title = "Maximum number of threads to use."
    )
    @PluginProperty
    @Builder.Default
    private final Integer maxWorkers = 8;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Quickbooks' username."
    )
    @PluginProperty(dynamic = true)
    private String realmId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Quickbooks' client ID."
    )
    @PluginProperty(dynamic = true)
    private String clientId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Quickbooks' client secret."
    )
    @PluginProperty(dynamic = true)
    private String clientSecret;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Quickbooks' refresh token."
    )
    @PluginProperty(dynamic = true)
    private String refreshToken;

    @NotNull
    @Schema(
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private LocalDate startDate;

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
            .put("select_fields_by_default", this.selectFieldsByDefault)
            .put("is_sandbox", this.isSandbox)
            .put("state_message_threshold", this.stateMessageThreshold)
            .put("max_workers", this.maxWorkers)
            .put("start_date", runContext.render(this.startDate.toString()))
            .put("realmId", runContext.render(this.realmId))
            .put("client_id", runContext.render(this.clientId))
            .put("client_secret", runContext.render(this.clientSecret))
            .put("refresh_token", runContext.render(this.refreshToken));

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("tap-quickbooks");
    }

    @Override
    protected String command() {
        return "tap-quickbooks";
    }
}
