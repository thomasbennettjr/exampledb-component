package com.exampledb.talend.components.source;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.exampledb.talend.components.service.I18nMessage;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exampledb.talend.components.service.ExampledbComponentService;

@Documentation("TODO fill the documentation for this source")
public class ExampleDBSource implements Serializable {
    private final ExampleDBSourceMapperConfiguration configuration;
    private final ExampledbComponentService service;
    private final RecordBuilderFactory builderFactory;
    private final I18nMessage i18n;
    private ExampledbComponentService.DataSource dataSource;
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private transient Schema schema;
    private static final transient Logger LOG = LoggerFactory.getLogger(ExampleDBSource.class);

    public ExampleDBSource(@Option("configuration") final ExampleDBSourceMapperConfiguration configuration,
                        final ExampledbComponentService service,
                        final RecordBuilderFactory builderFactory,
                           final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        // this method will be executed once for the whole component execution,
        // this is where you can establish a connection for instance
        try {
            dataSource = service.createDataSource(configuration.getDataset().getDatastore());
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.setFetchSize(1000);
            String table = configuration.getDataset().getTable();


            resultSet = statement.executeQuery("select * from " + configuration.getDataset().getTable());

        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Producer
    public Record next() {
        // this is the method allowing you to go through the dataset associated
        // to the component configuration
        //
        // return null means the dataset has no more data to go through
        // you can use the builderFactory to create a new Record.
        try {
            if (!resultSet.next()) {
                // No more records in ResultSet return null value back so Talend Engine knows the read is done
                return null;

            }

            final ResultSetMetaData metaData = resultSet.getMetaData();
            if (schema == null) {
                final Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(Schema.Type.RECORD);
                IntStream.rangeClosed(1, metaData.getColumnCount()).forEach(index -> addField(schemaBuilder, metaData, index));
                schema = schemaBuilder.build();
            }

            final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schema);
            IntStream.rangeClosed(1, metaData.getColumnCount()).forEach(index -> addColumn(recordBuilder, metaData, index));

            return recordBuilder.build();
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.warn(i18n.warnResultSetCantBeClosed(), e);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.warn(i18n.warnStatementCantBeClosed(), e);
            }
        }
        if (connection != null) {
            try {
                connection.commit();
            } catch (final SQLException e) {
                LOG.error(i18n.errorSQL(e.getErrorCode(), e.getMessage()), e);
                try {
                    connection.rollback();
                } catch (final SQLException rollbackError) {
                    LOG.error(i18n.errorSQL(rollbackError.getErrorCode(), rollbackError.getMessage()), rollbackError);
                }
            }
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn(i18n.warnConnectionCantBeClosed(), e);
            }
        }
        if (dataSource != null) {
            try {
                dataSource.close();
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }

    private void addField(final Schema.Builder builder, final ResultSetMetaData metaData, final int columnIndex) {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final int sqlType = metaData.getColumnType(columnIndex);
            final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
            entryBuilder.withName(metaData.getColumnName(columnIndex))
                    .withNullable(metaData.isNullable(columnIndex) != ResultSetMetaData.columnNoNulls);
            switch (sqlType) {
                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                    if (javaType.equals(Integer.class.getName())) {
                        builder.withEntry(entryBuilder.withType(Schema.Type.INT).build());
                    } else {
                        builder.withEntry(entryBuilder.withType(Schema.Type.LONG).build());
                    }
                    break;
                case Types.FLOAT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.REAL:
                    builder.withEntry(entryBuilder.withType(Schema.Type.FLOAT).build());
                    break;
                case Types.DOUBLE:
                    builder.withEntry(entryBuilder.withType(Schema.Type.DOUBLE).build());
                    break;
                case Types.BOOLEAN:
                    builder.withEntry(entryBuilder.withType(Schema.Type.BOOLEAN).build());
                    break;
                case Types.TIME:
                case Types.DATE:
                case Types.TIMESTAMP:
                    builder.withEntry(entryBuilder.withType(Schema.Type.DATETIME).build());
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    builder.withEntry(entryBuilder.withType(Schema.Type.BYTES).build());
                    break;
                case Types.BIGINT:
                    builder.withEntry(entryBuilder.withType(Schema.Type.LONG).build());
                    break;
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                default:
                    builder.withEntry(entryBuilder.withType(Schema.Type.STRING).build());
                    break;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void addColumn(final Record.Builder builder, final ResultSetMetaData metaData, final int columnIndex) {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final String integerType = Integer.class.getName();
            final int sqlType = metaData.getColumnType(columnIndex);
            final Object value = resultSet.getObject(columnIndex);
            final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
            entryBuilder.withName(metaData.getColumnName(columnIndex))
                    .withNullable(metaData.isNullable(columnIndex) != ResultSetMetaData.columnNoNulls);
            switch (sqlType) {
                case Types.SMALLINT:
                case Types.TINYINT:
                case Types.INTEGER:
                    if (value != null) {
                        if (javaType.equals(integerType)) {
                            builder.withInt(entryBuilder.withType(Schema.Type.INT).build(), (Integer) value);
                        } else {
                            builder.withLong(entryBuilder.withType(Schema.Type.LONG).build(), (Long) value);
                        }
                    }
                    break;
                case Types.FLOAT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.REAL:
                    if (value != null) {
                        builder.withFloat(entryBuilder.withType(Schema.Type.FLOAT).build(), javaType.equals("java.math.BigDecimal") ? ((BigDecimal) value).floatValue() : (Float) value);
                    }
                    break;
                case Types.DOUBLE:
                    if (value != null) {
                        builder.withDouble(entryBuilder.withType(Schema.Type.DOUBLE).build(), (Double) value);
                    }
                    break;
                case Types.BOOLEAN:
                    if (value != null) {
                        builder.withBoolean(entryBuilder.withType(Schema.Type.BOOLEAN).build(), (Boolean) value);
                    }
                    break;

                case Types.DATE:
                    builder.withDateTime(entryBuilder.withType(Schema.Type.DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Date) value).getTime()));
                    break;
                case Types.TIME:
                    builder.withDateTime(entryBuilder.withType(Schema.Type.DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Time) value).getTime()));
                    break;
                case Types.TIMESTAMP:
                    builder.withDateTime(entryBuilder.withType(Schema.Type.DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Timestamp) value).getTime()));
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    builder.withBytes(entryBuilder.withType(Schema.Type.BYTES).build(), value == null ? null : (byte[]) value);
                    break;
                case Types.BIGINT:
                    builder.withLong(entryBuilder.withType(Schema.Type.LONG).build(), (Long) value);
                    break;
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                default:
                    builder.withString(entryBuilder.withType(Schema.Type.STRING).build(), value == null ? null : String.valueOf(value));
                    break;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}