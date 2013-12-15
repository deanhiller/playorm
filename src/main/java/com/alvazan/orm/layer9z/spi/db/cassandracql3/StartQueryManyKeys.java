package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class StartQueryManyKeys implements StartQueryListener {

    private static final Logger log = LoggerFactory.getLogger(StartQueryManyKeys.class);
    private List<byte[]> values;
    private String keySpace;
    private byte[] rowKey;
    private Session session;
    private String indTable;
    private DboColumnMeta columnMeta;

    public StartQueryManyKeys(String keys, ScanInfo info, Session session2, List<byte[]> values, boolean reverse) {
        this.keySpace = keys;
        this.rowKey = info.getRowKey();
        this.indTable = info.getIndexColFamily();
        this.values = values;
        this.session = session2;
        this.columnMeta = info.getColumnName();
    }

    @Override
    public List<Future<ResultSet>> start() {

        List<Future<ResultSet>> futures = new ArrayList<Future<ResultSet>>();

        String rowKeyString = StandardConverters.convertFromBytes(String.class, rowKey);

        for (byte[] val : values) {
            Select selectQuery = QueryBuilder.select().all().from(keySpace, indTable).allowFiltering();
            Where selectWhere = selectQuery.where();
            Clause rkClause = QueryBuilder.eq("id", rowKeyString);
            selectWhere.and(rkClause);

            Object value = null;
            value = columnMeta.getStorageType().convertFromNoSql(val);
            value = Cql3Util.checkForBooleanAndNull(value, indTable, columnMeta);

            Clause valClause = QueryBuilder.eq("colname", value);
            selectWhere.and(valClause);

            Query query = selectWhere.disableTracing();

            Future future = session.executeAsync(query);
            futures.add(future);
        }

        return futures;
    }

}
