@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for using Singer taps.\n" +
        "Singer is a simple, composable, open source ETL.",
    categories = PluginSubGroup.PluginCategory.INGESTION
)
@Deprecated(since = "0.24")
package io.kestra.plugin.singer.taps;

import io.kestra.core.models.annotations.PluginSubGroup;