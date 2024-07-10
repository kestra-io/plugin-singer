package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

class ExchangeRateHostTest extends TapsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @Disabled("site down")
    void run() throws Exception {
        ExchangeRateHost.ExchangeRateHostBuilder<?, ?> builder = ExchangeRateHost.builder()
            .id(IdUtils.create())
            .type(ExchangeRateHost.class.getName())
            .startDate(LocalDate.now().minusDays(2))
            .type(ExchangeRateHost.class.getName());

        ExchangeRateHost task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        PipelinewiseMongoDb.Output output = task.run(runContext);

        Map<StreamType, List<Map<String, Object>>> groupedByType = groupedByType(runContext, output.getRaw());

        assertThat(groupedByType.get(StreamType.SCHEMA).size(), is(1));
        assertThat(groupedByType.get(StreamType.SCHEMA).get(0).get("stream"), is("exchange_rate"));
        assertThat(groupedByType.get(StreamType.RECORD).size(), greaterThan(0));
        assertThat(groupedByType.get(StreamType.STATE).size(), is(1));

        // rerun, we won't retrieve any record due to state
        String rawOutput = new BufferedReader(new InputStreamReader(runContext.storage().getFile(builder.build().run(runContext).getRaw()))).lines().collect(Collectors.joining("\n"));
        assertThat(rawOutput, is(""));
    }
}
