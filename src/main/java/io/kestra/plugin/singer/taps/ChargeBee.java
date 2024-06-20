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
    title = "A Singer tap to fetch data from a ChargeBee account.",
    description = "Full documentation can be found [here](https://github.com/hotgluexyz/tap-chargebee)"
)
public class ChargeBee extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Your Api Key."
    )
    @PluginProperty(dynamic = true)
    private String apiKey;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your site url.",
        description = "mostly in the form {site}.chargebee.com"
    )
    @PluginProperty(dynamic = true)
    private String site;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The version of product catalog wanted."
    )
    @PluginProperty(dynamic = true)
    private final String productCatalog = "1.0";

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
            .put("api_key", runContext.render(this.apiKey))
            .put("full_site", runContext.render(this.site))
            .put("product_catalog", runContext.render(this.productCatalog))
            .put("start_date", runContext.render(this.startDate.toString()));

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("git+https://github.com/hotgluexyz/tap-chargebee.git");
    }

    @Override
    protected String command() {
        return "tap-chargebee";
    }
}
