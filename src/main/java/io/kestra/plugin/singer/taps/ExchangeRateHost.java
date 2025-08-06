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
    title = "Fetch data from an exchangerate.host API with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/anelendata/tap-exchangeratehost)."
)
@Deprecated(forRemoval = true, since = "0.24")
public class ExchangeRateHost extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotEmpty
    @Schema(deprecated = true,
        title = "The exchange rates currency used for conversion."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final String base = "EUR";

    @NotNull
    @Schema(deprecated = true,
        title = "Determines how much historical data will be extracted.",
        description = "Please be aware that the larger the time period and amount of data, the longer the initial extraction can be expected to take."
    )
    @PluginProperty(dynamic = true)
    private LocalDate startDate;

    @Schema(deprecated = true,
        title = "Date up to when historical data will be extracted."
    )
    @PluginProperty(dynamic = true)
    private LocalDate endDate;

    public List<Feature> features() {
        return Collections.singletonList(
            Feature.STATE
        );
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("base", runContext.render(this.base))
            .put("start_date", runContext.render(this.startDate.toString()));

        if (this.endDate != null) {
            builder.put("end_date", runContext.render(this.endDate.toString()));
        }

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.ofValue(Collections.singletonList("tap-exchangeratehost"));
    }

    @Override
    protected Property<String> command() {
        return Property.ofValue("tap-exchangeratehost");
    }
}
