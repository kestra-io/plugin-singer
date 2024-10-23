package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.singer.models.DiscoverMetadata;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.time.Instant;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class BigQueryTest extends TapsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        URL resource = BigQueryTest.class.getClassLoader().getResource("gcp-service-account.json");
        String serviceAccount = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));

        BigQuery.BigQueryBuilder<?, ?> builder = BigQuery.builder()
            .id(IdUtils.create())
            .taskRunner(Process.instance())
            .type(BigQuery.class.getName())
            .serviceAccount(serviceAccount)
            .startAlwaysInclusive(false)
            .startDateTime(Instant.parse("2013-09-08T16:19:12Z"))
            .limit(1)
            .streams(Collections.singletonList(
                BigQuery.Stream.builder()
                    .name("covid19_nyt_us_states")
                    .table("bigquery-public-data.covid19_nyt.us_states")
                    .columns(Arrays.asList("date", "state_name", "confirmed_cases", "deaths"))
                    .datetimeKey("date")
                    .filters(Arrays.asList(
                        "EXTRACT (YEAR from date) = 2020",
                        "EXTRACT (MONTH from date) = 3",
                        "state_fips_code = '66'"
                    ))
                    .build()
            ))
            .streamsConfigurations(Collections.singletonList(
                StreamsConfiguration.builder()
                    .replicationMethod(DiscoverMetadata.ReplicationMethod.FULL_TABLE)
                    .selected(true)
                    .build()
            ));

        BigQuery task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        PipelinewiseMongoDb.Output output = task.run(runContext);

        Map<StreamType, List<Map<String, Object>>> groupedByType = groupedByType(runContext, output.getRaw());

        assertThat(groupedByType.get(StreamType.SCHEMA).size(), is(1));
        assertThat(groupedByType.get(StreamType.SCHEMA).get(0).get("stream"), is("covid19_nyt_us_states"));
        assertThat(groupedByType.get(StreamType.RECORD).size(), is(17));
        assertThat(groupedByType.get(StreamType.STATE).size(), is(2));

        // rerun, since we have startAlwaysInclusive false, we won't retrieve any record
        assertThat(groupedByType(runContext, builder.build().run(runContext).getRaw()).containsKey(StreamType.RECORD), is(false));
    }
}
