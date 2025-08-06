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
@Schema(deprecated = true,
    title = "Fetch data from a Recharge account with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/singer-io/tap-recharge)."
)
@Deprecated(forRemoval = true, since="0.24")
public class Recharge extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(deprecated = true,
        title = "Private API Token."
    )
    @PluginProperty(dynamic = true)
    private String accessToken;

    @Schema(deprecated = true,
        title = "User agent.",
        description = "User agent to send to ReCharge along with API requests. " +
            "Typically includes name of integration and an email address you can be reached at."
    )
    private Property<String> userAgent;

    @NotNull
    @Schema(deprecated = true,
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
            .put("access_token", runContext.render(this.accessToken))
            .put("user_agent", runContext.render(this.userAgent).as(String.class).orElseThrow())
            .put("start_date", runContext.render(this.startDate.toString()));

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("tap-recharge"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-recharge");
    }
}
