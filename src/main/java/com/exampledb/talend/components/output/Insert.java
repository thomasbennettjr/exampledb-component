package com.exampledb.talend.components.output;

import com.exampledb.talend.components.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Insert extends QueryManagerImpl {

    private Map<Integer, Schema.Entry> namedParams;

    private final Map<String, String> queries = new HashMap<>();

    public Insert(final ExampleDBOutputConfiguration configuration, final I18nMessage i18n) {
        super(i18n, configuration);
        namedParams = new HashMap<>();
    }

    @Override
    public String buildQuery(final List<Record> records) {
        final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                .collect(Collectors.toList());
        return queries.computeIfAbsent(entries.stream().map(Schema.Entry::getName).collect(Collectors.joining("::")), key -> {
            final AtomicInteger index = new AtomicInteger(0);
            // namedParams = new HashMap<>();
            entries.forEach(name -> namedParams.put(index.incrementAndGet(), name));
            final List<Map.Entry<Integer, Schema.Entry>> params = namedParams.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey)).collect(Collectors.toList());
            final StringBuilder query = new StringBuilder("INSERT INTO ")
                    .append(identifier(getConfiguration().getDataset().getTable()));
            query.append(params.stream().map(e -> e.getValue().getName()).map(name -> identifier(name))
                    .collect(Collectors.joining(",", "(", ")")));
            query.append(" VALUES");
            query.append(params.stream().map(e -> "?").collect((Collectors.joining(",", "(", ")"))));
            return query.toString();
        });
    }

    @Override
    public boolean validateQueryParam(final Record record) {
        return namedParams.values().stream().filter(e -> !e.isNullable()).map(e -> valueOf(record, e))
                .allMatch(Optional::isPresent);
    }

    @Override
    public Map<Integer, Schema.Entry> getQueryParams() {
        //return namedParams;
        return Collections.unmodifiableMap(namedParams);
    }

    @Override
    public void load(final Connection connection) throws SQLException {}
}
