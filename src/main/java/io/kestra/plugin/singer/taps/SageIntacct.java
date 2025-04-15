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
@Schema(
    title = "Fetch data from a Sage Intacct account with a Singer tap.",
    description = "Full documentation can be found on the [GitHub Repo](https://github.com/hotgluexyz/tap-intacct)."
)
public class SageIntacct extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Company Id."
    )
    @PluginProperty(dynamic = true)
    private String companyId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Intacct Sender ID."
    )
    @PluginProperty(dynamic = true)
    private String senderId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Intacct Sender Password."
    )
    @PluginProperty(dynamic = true)
    private String senderPassword;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Intacct User ID."
    )
    @PluginProperty(dynamic = true)
    private String userId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Intacct User Password."
    )
    @PluginProperty(dynamic = true)
    private String userPassword;

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
            .put("company_id", runContext.render(this.companyId))
            .put("sender_id", runContext.render(this.senderId))
            .put("sender_password", runContext.render(this.senderPassword))
            .put("user_id", runContext.render(this.userId))
            .put("user_password", runContext.render(this.userPassword))
            .put("start_date", runContext.render(this.startDate.toString()));

        return builder.build();
    }

    @Override
    public Property<List<String>> pipPackages() {
        return Property.of(Collections.singletonList("git+https://github.com/hotgluexyz/tap-intacct.git"));
    }

    @Override
    protected Property<String> command() {
        return Property.of("tap-intacct");
    }
}
