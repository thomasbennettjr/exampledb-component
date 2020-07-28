package com.exampledb.talend.components.output;

import com.exampledb.talend.components.service.ExampledbComponentService;
import org.talend.sdk.component.api.record.Record;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

public interface QueryManager extends Serializable {

    List<Reject> execute(List<Record> records, ExampledbComponentService.DataSource dataSource) throws SQLException, IOException;
}
