package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
@Schema(deprecated = true,
    title = "Fetch data from a Quickbooks account with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/hotgluexyz/tap-quickbooks.git)."
)
@Deprecated(forRemoval = true, since="0.24")
public class Quickbooks extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @Schema(deprecated = true,
        title = "Select by default any new fields discovered in Quickbooks objects."
    )
    @PluginProperty
    @Builder.Default
    private final Property<Boolean> selectFieldsByDefault = Property.ofValue(true);

    @NotNull
    @Schema(deprecated = true,
        title = "Select by default any new fields discovered in Quickbooks objects."
    )
    @PluginProperty
    @Builder.Default
    private final Property<Boolean> isSandbox = Property.ofValue(false);

    @NotNull
    @Schema(deprecated = true,
        title = "Generate a STATE message every N records."
    )
    @Builder.Default
    private final Property<Integer> stateMessageThreshold = Property.ofValue(1000);

    @NotNull
    @Schema(deprecated = true,
        title = "Maximum number of threads to use."
    )
    @Builder.Default
    private final Property<Integer> maxWorkers = Property.ofValue(8);

    @NotNull
    @NotEmpty
    @Schema(deprecated = true,
        title = "Quickbooks' username."
    )
    @PluginProperty(dynamic = true)
    private String realmId;

    @NotNull
    @NotEmpty
    @Schema(deprecated = true,
        title = "Quickbooks' client ID."
    )
    @PluginProperty(dynamic = true)
    private String clientId;

    @NotNull
    @NotEmpty
    @Schema(deprecated = true,
        title = "Quickbooks' client secret."
    )
    @PluginProperty(dynamic = true)
    private String clientSecret;

    @NotNull
    @NotEmpty
    @Schema(deprecated = true,
        title = "Quickbooks' refresh token."
    )
    @PluginProperty(dynamic = true)
    private String refreshToken;

    @NotNull
    @Schema(deprecated = true,
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
            .put("select_fields_by_default", runContext.render(this.selectFieldsByDefault).as(Boolean.class).orElseThrow())
            .put("is_sandbox", runContext.render(this.isSandbox).as(Boolean.class).orElseThrow())
            .put("state_message_threshold", runContext.render(this.stateMessageThreshold).as(Integer.class).orElseThrow())
            .put("max_workers", runContext.render(this.maxWorkers).as(Integer.class).orElseThrow())
            .put("start_date", runContext.render(this.startDate.toString()))
            .put("realmId", runContext.render(this.realmId))
            .put("client_id", runContext.render(this.clientId))
            .put("client_secret", runContext.render(this.clientSecret))
            .put("refresh_token", runContext.render(this.refreshToken));

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("tap-quickbooks"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-quickbooks");
    }
}
