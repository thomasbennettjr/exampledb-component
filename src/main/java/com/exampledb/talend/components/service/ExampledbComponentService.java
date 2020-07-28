package com.exampledb.talend.components.service;

import com.exampledb.talend.components.datastore.ExampleDBDatastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.sql.*;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import static java.util.Optional.ofNullable;


@Service
public class ExampledbComponentService {

    private static final transient Logger LOG = LoggerFactory.getLogger(ExampledbComponentService.class);

    @Service
    private I18nMessage i18n;


    @HealthCheck("validateConnection")
    public HealthCheckStatus validateConnection(@Option final ExampleDBDatastore datastore)
    {
        DataSource dataSource = createDataSource(datastore);
        try {
            dataSource.createConnection();
            dataSource.testConnection();
            return new HealthCheckStatus(HealthCheckStatus.Status.OK,i18n.successConnection());
        } catch(SQLException e)
        {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO,i18n.errorInvalidConnection());
        }
    }

    @Suggestions("listTables")
    public SuggestionValues getTableFromDatabase(@Option final ExampleDBDatastore datastore) {
        final Collection<SuggestionValues.Item> items = new HashSet<>();
        try (Connection connection = createDataSource(datastore).getConnection()) {
            final DatabaseMetaData dbMetaData = connection.getMetaData();
            try (ResultSet tables = dbMetaData.getTables(connection.getCatalog(), connection.getSchema(), null,
                    getAvailableTableTypes(dbMetaData).toArray(new String[0]))) {
                while (tables.next()) {
                    ofNullable(ofNullable(tables.getString("TABLE_NAME")).orElseGet(() -> {
                        try {
                            return tables.getString("SYNONYM_NAME");
                        } catch (final SQLException e) {
                            return null;
                        }
                    })).ifPresent(t -> items.add(new SuggestionValues.Item(t, t)));
                }
            }
        } catch (final Exception unexpected) { // catch all exceptions for this ui label to return empty list
            LOG.error(i18n.errorCantLoadTableSuggestions(), unexpected);
        }
        return new SuggestionValues(true, items);
    }

    private Set<String> getAvailableTableTypes(DatabaseMetaData dbMetaData) throws SQLException {
        Set<String> result = new HashSet<>();
        try (ResultSet tables = dbMetaData.getTableTypes()) {
            while (tables.next()) {
                ofNullable(tables.getString("TABLE_TYPE")).map(String::trim)
                        .map(t -> ("BASE TABLE".equalsIgnoreCase(t)) ? "TABLE" : t)
                        .filter(t -> getSupportedTableTypes().contains(t)).ifPresent(result::add);
            }
        }
        return result;
    }

    private Set<String> getSupportedTableTypes()
    {
        Set<String> tableTypes = new HashSet<>();
        tableTypes.add("TABLE");
        tableTypes.add("VIEW");
        tableTypes.add("SYNONYM");

        return tableTypes;
    }


    public DataSource createDataSource(final ExampleDBDatastore connection)
    {

        return new DataSource(i18n, connection, false, false);
    }

    public DataSource createDataSource(final ExampleDBDatastore connection, final boolean rewriteBatchedStatements) {
        return new DataSource(i18n, connection, false, rewriteBatchedStatements);
    }

    public DataSource createDataSource(final ExampleDBDatastore connection, boolean isAutoCommit,
                                           final boolean rewriteBatchedStatements) {

        return new DataSource(i18n, connection, isAutoCommit, rewriteBatchedStatements);
    }

    public static class DataSource implements AutoCloseable
    {
        private Connection connection;
        private final I18nMessage i18nMessage;
        private final ExampleDBDatastore datastore;
        private final boolean isAutoCommit;
        private final boolean rewriteBatchedStatements;
        public DataSource(final I18nMessage i18nMessage, final ExampleDBDatastore datastore,
                          final boolean isAutoCommit, final boolean rewriteBatchedStatements)
        {
            this.i18nMessage = i18nMessage;
            this.datastore = datastore;
            this.isAutoCommit = isAutoCommit;
            this.rewriteBatchedStatements = rewriteBatchedStatements;
        }

        private void createConnection()
        {
            try {
                Class.forName("org.postgresql.Driver");
                connection = DriverManager.getConnection("jdbc:postgresql://"+datastore.getServer()+":"+datastore.getPort()+"/"+datastore.getDatabase()
                                + "?rewriteBatchedStatements=" + String.valueOf(rewriteBatchedStatements),
                        datastore.getUsername(), datastore.getPassword());
                connection.setAutoCommit(isAutoCommit);

            } catch (ClassNotFoundException | SQLException e) {
                LOG.error(e.getMessage());
            }
        }

        public Connection getConnection() {
            try {
                if (connection == null || connection.isClosed())
                    createConnection();
            } catch (SQLException e) {
                LOG.error(e.getMessage());
            }

            return this.connection;
        }

        public void testConnection() throws SQLException {
            connection.isClosed();
        }

        @Override
        public void close() throws Exception {
            connection.close();
        }
    }
}