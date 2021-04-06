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
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "A Singer tap to fetch data from a Sage Intacct account.",
    description = "Full documentation can be found [here](https://github.com/hotgluexyz/tap-intacct)"
)
public class SageIntacct extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Your Api Key."
    )
    @PluginProperty(dynamic = true)
    private String company_id;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Your site url.",
        description = "mostly in the form {site}.chargebee.com"
    )
    @PluginProperty(dynamic = true)
    private String sender_id;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The version of product catalog wanted."
    )
    @PluginProperty(dynamic = true)
    private String sender_password;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The version of product catalog wanted."
    )
    @PluginProperty(dynamic = true)
    private String user_id;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The version of product catalog wanted."
    )
    @PluginProperty(dynamic = true)
    private String user_password;


    @NotNull
    @NotEmpty
    @Schema(
        title = "The version of product catalog wanted."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final Boolean select_fields_by_default = true;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The version of product catalog wanted."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final Integer state_message_threshold = 1000;

    @NotNull
    @NotEmpty
    @Schema(
        title = "The version of product catalog wanted."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final Integer max_workers = 8;

    @NotNull
    @NotEmpty
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
            .put("company_id", runContext.render(this.company_id))
            .put("sender_id", runContext.render(this.sender_id))
            .put("sender_password", runContext.render(this.sender_password))
            .put("user_id", runContext.render(this.user_id))
            .put("user_password", runContext.render(this.user_password))
            .put("select_fields_by_default", this.select_fields_by_default)
            .put("state_message_threshold", this.state_message_threshold)
            .put("max_workers", this.max_workers)
            .put("start_date", runContext.render(this.startDate.toString()));

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("git+https://github.com/hotgluexyz/tap-intacct.git");
    }

    @Override
    protected String command() {
        return "tap-intacct";
    }
}
