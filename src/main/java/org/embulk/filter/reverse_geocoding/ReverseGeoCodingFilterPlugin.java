package org.embulk.filter.reverse_geocoding;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.embulk.filter.reverse_geocoding.ReverseGeoCodingFilterPlugin.OutputColumnTask.TYPE_CITY;
import static org.embulk.filter.reverse_geocoding.ReverseGeoCodingFilterPlugin.OutputColumnTask.TYPE_HASH;
import static org.embulk.filter.reverse_geocoding.ReverseGeoCodingFilterPlugin.OutputColumnTask.TYPE_PREF;

public class ReverseGeoCodingFilterPlugin
        implements FilterPlugin
{
    static Schema buildOutputSchema(PluginTask task, Schema inputSchema)
    {
        List<OutputColumnTask> outputColumns = task.getOutputColumns();
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        builder.addAll(inputSchema.getColumns());
        int i = inputSchema.getColumnCount();

        for (OutputColumnTask outputColumn : outputColumns) {
            Column column = new Column(i++, outputColumn.getName(), outputColumn.getType());
            builder.add(column);
        }

        return new Schema(builder.build());
    }

    static Map<String, OutputColumnTask> getTaskColumnMap(List<OutputColumnTask> outputColumnTasks)
    {
        Map<String, OutputColumnTask> m = new HashMap<>();
        for (OutputColumnTask columnTask : outputColumnTasks) {
            m.put(columnTask.getName(), columnTask);
        }
        return m;
    }

    static Map<String, Column> getOutputSchemaMap(Schema outputSchema)
    {
        Map<String, Column> m = new HashMap<>();
        for (Column column : outputSchema.getColumns()) {
            m.put(column.getName(), column);
        }
        return m;
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema outputSchema = buildOutputSchema(task, inputSchema);
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {

        final PluginTask task = taskSource.loadTask(PluginTask.class);

        final Map<String, Column> outputSchemaMap = getOutputSchemaMap(outputSchema);

        return new PageOutput()
        {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(
                    Exec.getBufferAllocator(), outputSchema, output);
            private ColumnVisitorImpl visitor = new ColumnVisitorImpl(builder);

            @Override
            public void add(Page page)
            {
                reader.setPage(page);

                while (reader.nextRecord()) {
                    double lon = 0d;
                    double lat = 0d;

                    // get lon / lat
                    for (Column column : inputSchema.getColumns()) {
                        String colName = column.getName();
                        if (colName.equals(task.getTargetLon())) {
                            lon = reader.getDouble(column);
                        }
                        else if (colName.equals(task.getTargetLat())) {
                            lat = reader.getDouble(column);
                        }
                        column.visit(visitor);
                    }

                    for (OutputColumnTask outputColumnTask : task.getOutputColumns()) {
                        String name = outputColumnTask.getName();
                        String output = outputColumnTask.getOutput(); // pref/city

                        int idx = outputSchemaMap.get(name).getIndex();
                        Type type = outputColumnTask.getType();

                        if (Types.STRING.equals(type)) {

                            if (output.equals(TYPE_PREF)) {
                                builder.setString(idx, GeoCodeMap.convert2Pref(lat, lon));
                            }
                            else if (output.equals(TYPE_CITY)) {
                                builder.setString(idx, GeoCodeMap.convert2City(lat, lon));
                            }
                            else if (output.equals(TYPE_HASH)) {
                                builder.setString(idx, GeoCodeMap.convert2GeoHash(lat, lon));
                            }
                        }
                        else if (Types.DOUBLE.equals(type)) {
                            builder.setDouble(idx, GeoCodeMap.convert2LatLon(hash));
                        }
                    }

                    //visited columns
                    builder.addRecord();
                }
            }

            @Override
            public void finish()
            {
                builder.finish();
            }

            @Override
            public void close()
            {
                builder.close();
            }

            class ColumnVisitorImpl
                    implements ColumnVisitor
            {
                private final PageBuilder builder;

                ColumnVisitorImpl(PageBuilder builder)
                {
                    this.builder = builder;
                }

                @Override
                public void booleanColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setBoolean(outputColumn, reader.getBoolean(outputColumn));
                    }
                }

                @Override
                public void longColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setLong(outputColumn, reader.getLong(outputColumn));
                    }
                }

                @Override
                public void doubleColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setDouble(outputColumn, reader.getDouble(outputColumn));
                    }
                }

                @Override
                public void stringColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setString(outputColumn, reader.getString(outputColumn));
                    }
                }

                @Override
                public void timestampColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setTimestamp(outputColumn, reader.getTimestamp(outputColumn));
                    }
                }

                @Override
                public void jsonColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setJson(outputColumn, reader.getJson(outputColumn));
                    }
                }
            }
        };
    }

    public interface PluginTask
            extends Task
    {
        @Config("target_lon")
        public String getTargetLon();

        @Config("target_lat")
        public String getTargetLat();

        @Config("level")
        @ConfigDefault("5")
        public Optional<String> getLevel();

        @Config("output_columns")
        public List<OutputColumnTask> getOutputColumns();
    }

    public interface OutputColumnTask
            extends Task
    {
        String TYPE_PREF = "pref";
        String TYPE_CITY = "city";
        String TYPE_HASH = "hash";

        @Config("name")
        String getName();

        @Config("type")
        Type getType();

        @Config("out")
        @ConfigDefault("pref")
        String getOutput();
    }
}
