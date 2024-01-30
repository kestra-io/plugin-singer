package io.kestra.plugin.singer.taps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.singer.models.Feature;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "A Singer tap to fetch data from a crow.dev api.",
    description = "Full documentation can be found [here](https://github.com/edgarrmondragon/tap-crowd-dev)"
)
public class CrowdDev extends AbstractPythonTap implements RunnableTask<AbstractPythonTap.Output> {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Tenant ID for Crowd Dev"
    )
    @PluginProperty(dynamic = true)
    private String tenantId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "API Token for Crowd Dev"
    )
    @PluginProperty(dynamic = true)
    private String token;

    @Schema(
        title = "List Config object for stream maps capability.",
        description = "For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html)."
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> streamMaps;

    @Schema(
        title = "User-defined config values to be used within map expressions."
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> streamMapConfig;

    @Schema(
        title = "To enable schema flattening and automatically expand nested properties."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Boolean flatteningEnabled = false;

    @Schema(
        title = "The max depth to flatten schemas."
    )
    @PluginProperty(dynamic = true)
    private Integer flatteningMaxDepth;

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
            Feature.CATALOG,
            Feature.DISCOVER,
            Feature.STATE
        );
    }

    @Override
    public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("tenant_id", runContext.render(this.tenantId))
            .put("token", runContext.render(this.token))
            .put("start_date", runContext.render(this.startDate.toString()));

        try {
            if (this.streamMaps != null) {
                builder.put("stream_maps", JacksonMapper.ofJson().writeValueAsString(runContext.render(this.streamMaps)));
            }

            if (this.streamMapConfig != null) {
                builder.put("stream_map_config", JacksonMapper.ofJson().writeValueAsString(runContext.render(this.streamMapConfig)));
            }
        } catch (JsonProcessingException e) {
            throw new IllegalVariableEvaluationException(e);
        }

        if (this.flatteningEnabled != null) {
            builder.put("flattening_enabled", this.flatteningEnabled);
        }

        if (this.flatteningMaxDepth != null) {
            builder.put("flattening_max_depth", this.flatteningMaxDepth);
        }

        return builder.build();
    }

    @Override
    public List<String> pipPackages() {
        return Collections.singletonList("git+https://github.com/edgarrmondragon/tap-crowd-dev.git");
    }

    @Override
    protected String command() {
        return "tap-crowd-dev";
    }
}
