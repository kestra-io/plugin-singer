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
    title = "Fetch data from a Stripe account with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/meltano/tap-stripe.git)."
)
@Deprecated(forRemoval = true, since="0.24")
public class Stripe extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(deprecated = true,
        title = "Stripe account ID.",
        description = "Ex. acct_1a2b3c4d5e"
    )
    @PluginProperty(dynamic = true)
    private String accountId;

    @NotNull
    @NotEmpty
    @Schema(deprecated = true,
        title = "Stripe secret API key."
    )
    @PluginProperty(dynamic = true)
    private String clientSecret;

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
            .put("account_id", runContext.render(this.accountId))
            .put("client_secret", runContext.render(this.clientSecret))
            .put("start_date", runContext.render(this.startDate.toString()));

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("git+https://github.com/meltano/tap-stripe.git"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-stripe");
    }
}
