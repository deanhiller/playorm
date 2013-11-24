package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.ColumnSliceInfo;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.SpiConstants;
import com.alvazan.orm.api.z8spi.action.Action;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.action.Persist;
import com.alvazan.orm.api.z8spi.action.PersistIndex;
import com.alvazan.orm.api.z8spi.action.Remove;
import com.alvazan.orm.api.z8spi.action.RemoveColumn;
import com.alvazan.orm.api.z8spi.action.RemoveIndex;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

public class CqlSession implements NoSqlRawSession {
    private static final Logger log = LoggerFactory.getLogger(CqlSession.class);

    private Session session = null;
    private Cluster cluster = null;
    private KeyspaceMetadata keyspaces = null;
    private String keys = "cql70";
    @Inject
    private Provider<Row> rowProvider;


    @Override
    public void start(Map<String, Object> properties) {

        String cqlSeed = "localhost";
        cluster = Cluster.builder().addContactPoint(cqlSeed).build();
        session = cluster.connect();
        keyspaces = cluster.getMetadata().getKeyspace(keys);
        if (keyspaces == null) {
            try {
                session.execute("CREATE KEYSPACE " + keys + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};");
            } catch (Exception e) {
                System.out.println("Exception :" + e.getMessage());
            }
        }
        session = cluster.connect(keys);
    }

    @Override
    public void sendChanges(List<Action> actions, MetaLookup ormSession) {
        for (Action action : actions) {
            if (action instanceof Persist) {
                persist((Persist) action, ormSession);
            } else if (action instanceof PersistIndex) {
                persistIndex((PersistIndex) action, ormSession);
            } else if(action instanceof Remove) {
                remove((Remove)action, ormSession);
            } else if(action instanceof RemoveIndex) {
                removeIndex((RemoveIndex) action, ormSession);
            } else if(action instanceof RemoveColumn) {
                removeColumn((RemoveColumn) action, ormSession);
            }

        }
    }

    private void persist(Persist action, MetaLookup ormSession) {
        StorageTypeEnum type = action.getColFamily().getNameStorageType();
        String colFamily = action.getColFamily().getColumnFamily();
        String table = lookupOrCreate(colFamily, ormSession);
        List<Column> s = action.getColumns();
        byte[] rowkey = action.getRowKey();
        byte[] nullArray  = StandardConverters.convertToBytes(SpiConstants.NULL_STRING_FORCQL3);

        for (Column c : s) {
            try {
                String colValue = StandardConverters.convertFromBytes(String.class, c.getValue());
                PreparedStatement statement = session.prepare("INSERT INTO " + keys + "." + table + "(id, colname, colvalue) VALUES (?, ?, ?)");
                BoundStatement boundStatement = new BoundStatement(statement);
                if (colValue != null) {
                    session.execute(boundStatement.bind(StandardConverters.convertFromBytes(String.class, rowkey),
                            StandardConverters.convertFromBytes(String.class, c.getName()),
                            ByteBuffer.wrap(c.getValue())));
                } else {
                    session.execute(boundStatement.bind(StandardConverters.convertFromBytes(String.class, rowkey),
                            StandardConverters.convertFromBytes(String.class, c.getName()), ByteBuffer.wrap(nullArray)));
                }

            } catch (Exception e) {
                System.out.println(c.getValue() + "Exception:" + e.getMessage());
            }
        }

    }

    private void persistIndex(PersistIndex action, MetaLookup ormSession) {
        String indexCfName = action.getIndexCfName();
        String table = lookupOrCreate(indexCfName, ormSession);
        byte[] rowKey = action.getRowKey();
        IndexColumn column = action.getColumn();
        byte[] key = column.getIndexedValue();
        byte[] value = column.getPrimaryKey();

        try {

            Object keyObject = null;
            if (key != null) {
                PreparedStatement statement = session.prepare("INSERT INTO " + keys + "." + table + "(id, colname, colvalue) VALUES (?, ?, ?)");
                BoundStatement boundStatement = new BoundStatement(statement);
                if (indexCfName.equalsIgnoreCase("StringIndice")) {
                    keyObject = StandardConverters.convertFromBytes(String.class, key);
                } else if (indexCfName.equalsIgnoreCase("IntegerIndice")) {
                    keyObject = StandardConverters.convertFromBytes(Long.class, key);
                } else if (indexCfName.equalsIgnoreCase("DecimalIndice")) {
                    keyObject = StandardConverters.convertFromBytes(Float.class, key);
                }
                session.execute(boundStatement.bind(StandardConverters.convertFromBytes(String.class, rowKey), keyObject, ByteBuffer.wrap(value)));
            } else {
                PreparedStatement statement = session.prepare("INSERT INTO " + keys + "." + table + "(id, colname, colvalue) VALUES (?, ?, ?)");
                BoundStatement boundStatement = new BoundStatement(statement);
                if (indexCfName.equalsIgnoreCase("IntegerIndice")) {
                    boundStatement.setString("id", StandardConverters.convertFromBytes(String.class, rowKey));
                    boundStatement.setBytesUnsafe("colname", ByteBuffer.wrap(new byte[0]));
                    boundStatement.setBytes("colvalue", ByteBuffer.wrap(value));
                    session.execute(boundStatement);
                } else
                    session.execute(boundStatement.bind(StandardConverters.convertFromBytes(String.class, rowKey), "", ByteBuffer.wrap(value)));
          }
        } catch (Exception e) {
            System.out.println(indexCfName + " Exception:" + e.getMessage());
        }

    }

    private void remove(Remove action, MetaLookup ormSession) {
        String colFamily = action.getColFamily().getColumnFamily();
        String table = lookupOrCreate(colFamily, ormSession);
        if (action.getAction() == null)
            throw new IllegalArgumentException("action param is missing ActionEnum so we know to remove entire row or just columns in the row");
        switch (action.getAction()) {
        case REMOVE_ENTIRE_ROW:
            String rowKey = StandardConverters.convertFromBytes(String.class, action.getRowKey());
            Clause eqClause = QueryBuilder.eq("id", rowKey);
            Query query = QueryBuilder.delete().from(keys, table).where(eqClause);
            session.execute(query);
            break;
        case REMOVE_COLUMNS_FROM_ROW:
            removeColumns(action, table);
            break;
        default:
            throw new RuntimeException("bug, unknown remove action=" + action.getAction());
        }
    }

    private void removeColumns(Remove action, String table) {
        String rowKey = StandardConverters.convertFromBytes(String.class, action.getRowKey());
        if (rowKey != null) {
            for (byte[] name : action.getColumns()) {
                String colName = StandardConverters.convertFromBytes(String.class, name);
                removeColumnImpl(rowKey, table, colName);
            }
        }
    }

    private void removeColumn(RemoveColumn action, MetaLookup ormSession) {
        String colFamily = action.getColFamily().getColumnFamily();
        String table = lookupOrCreate(colFamily, ormSession);
        String rowKey = StandardConverters.convertFromBytes(String.class, action.getRowKey());
        if (rowKey != null) {
            String colName = StandardConverters.convertFromBytes(String.class, action.getColumn());
            removeColumnImpl(rowKey, table, colName);
        }
    }

    private void removeColumnImpl(String rowKey, String table, String colName) {
        Clause eqClause = QueryBuilder.eq("id", rowKey);
        Clause eqColClause = QueryBuilder.eq("colname", colName);
        Query query = QueryBuilder.delete().from(keys, table).where(eqClause).and(eqColClause);
        session.execute(query);
    }

    private void removeIndex(RemoveIndex action, MetaLookup ormSession) {
        String colFamily = action.getIndexCfName();
        if (colFamily.equalsIgnoreCase("BytesIndice"))
            return;
        String table = lookupOrCreate(colFamily, ormSession);
        String rowKey = StandardConverters.convertFromBytes(String.class, action.getRowKey());
        IndexColumn column = action.getColumn();
        byte[] fk = column.getPrimaryKey();
        byte[] indexedValue = action.getColumn().getIndexedValue();
        Object indValue = null;
        if (table.equalsIgnoreCase("StringIndice"))
            indValue = StandardConverters.convertFromBytes(String.class, indexedValue);
        else if (table.equalsIgnoreCase("IntegerIndice"))
            indValue = StandardConverters.convertFromBytes(Integer.class, indexedValue);
        else if (table.equalsIgnoreCase("DecimalIndice"))
            indValue = StandardConverters.convertFromBytes(BigDecimal.class, indexedValue);
        boolean exists = findIndexRow(table, rowKey, fk, indValue);
        if (!exists) {
            if (log.isInfoEnabled())
                log.info("Index: " + column.toString() + " already removed.");
        } else {
            Clause eqClause = QueryBuilder.eq("id", rowKey);
            Clause indClause = null;
            if (indValue != null) {
                indClause = QueryBuilder.eq("colname", indValue);
            } else {
                if (table.equalsIgnoreCase("IntegerIndice")) {
                    indClause = QueryBuilder.eq("colname", ByteBuffer.wrap(new byte[0]));
                } else {
                    indClause = QueryBuilder.eq("colname", "");
                }
            }
            Clause fkClause = QueryBuilder.eq("colvalue", ByteBuffer.wrap(fk));
            Query query = QueryBuilder.delete().from(keys, table).where(eqClause).and(indClause).and(fkClause);
            session.execute(query);
        }
    }

    public boolean findIndexRow(String table, String rowKey, byte[] key, Object indValue) {
        Select selectQuery = QueryBuilder.select().all().from(keys, table).allowFiltering();
        Where selectWhere = selectQuery.where();
        Clause rkClause = QueryBuilder.eq("id", rowKey);
        selectWhere.and(rkClause);
        Clause indClause = null;
        if (indValue != null) {
            indClause = QueryBuilder.eq("colname", indValue);
        } else {
            if (table.equalsIgnoreCase("IntegerIndice")) {
                indClause = QueryBuilder.eq("colname", ByteBuffer.wrap(new byte[0]));
            } else {
                indClause = QueryBuilder.eq("colname", "");
            }
        }
        selectWhere.and(indClause);
        Clause keyClause = QueryBuilder.eq("colvalue", ByteBuffer.wrap(key));
        selectWhere.and(keyClause);
        Query query = selectWhere.limit(1);
        ResultSet resultSet = session.execute(query);
        return !resultSet.isExhausted();
   }

    private String lookupOrCreate(String colFamily1, MetaLookup ormSession) {
        if (cluster.getMetadata().getKeyspace(keys).getTable(colFamily1.toLowerCase()) == null) {
            try {
                String colType = null;
                if (colFamily1.equalsIgnoreCase("StringIndice")) {
                    colType = "colname text,";
                } else if (colFamily1.equalsIgnoreCase("IntegerIndice")) {
                    colType = "colname bigint,";
                } else if (colFamily1.equalsIgnoreCase("DecimalIndice")) {
                    colType = "colname float,";
                } else {
                    colType = "colname text,";
                }
                session.execute("CREATE TABLE " + keys + "." + colFamily1 + " (id text," + colType + "colvalue blob,"
                        + "PRIMARY KEY (id,colname, colvalue)" + ") WITH COMPACT STORAGE");

            } catch (Exception e) {
                System.out.println("Excepion in creating table" + colFamily1 + " creation:" + e.getMessage());

            }
        }
        return colFamily1;
    }

    @Override
    public void clearDatabase() {
        session.execute("DROP KEYSPACE " + keys);
        keyspaces = cluster.getMetadata().getKeyspace(keys);
        if (keyspaces == null) {
            try {
                session.execute("CREATE KEYSPACE " + keys + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};");
            } catch (Exception e) {
                System.out.println("Exception :" + e.getMessage());
            }
        }
        session = cluster.connect(keys);
    }

    @Override
    public void close() {
        //cluster.shutdown();
    }

    @Override
    public AbstractCursor<Column> columnSlice(ColumnSliceInfo sliceInfo, Integer batchSize, BatchListener l, MetaLookup mgr) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AbstractCursor<IndexColumn> scanIndex(ScanInfo scan, Key from, Key to, Integer batchSize, BatchListener l, MetaLookup mgr) {
        byte[] rowKey = scan.getRowKey();
        String indexTableName = scan.getIndexColFamily();
        DboColumnMeta colMeta = scan.getColumnName();
        DboTableMeta entityDbCollection = scan.getEntityColFamily();

        // Here we don't bother using an index at all since there is no where clause to begin with
        // ALSO, we don't want this code to be the case if we are doing a CursorToMany which has to
        // use an index so check the column type
/*        if (!entityDbCollection.isVirtualCf() && from == null && to == null && !(scan.getColumnName() instanceof DboColumnToManyMeta)
                && !entityDbCollection.isInheritance()) {
            ScanMongoDbCollection scanner = new ScanMongoDbCollection(batchSize, l, entityDbCollection.getColumnFamily(), db);
            scanner.beforeFirst();
            return scanner;
        }
*/
        CursorOfIndexes cursor = new CursorOfIndexes(rowKey, batchSize, l, indexTableName, from, to);
        cursor.setupMore(keys, colMeta, session);
        return cursor;
    }

    @Override
    public AbstractCursor<IndexColumn> scanIndex(ScanInfo scanInfo, List<byte[]> values, BatchListener list, MetaLookup mgr) {
        StartQueryListener listener = new StartQueryManyKeys(keys, scanInfo, session, values, false);
        //CursorForValues cursor = new CursorForValues(scanInfo, list, values, session, keys, listener);
        //return cursor;
        return new CursorOfFutures(listener, list, scanInfo);
    }

    @Override
    public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily, DirectCursor<byte[]> rowKeys, Cache cache, int batchSize, BatchListener list,
            MetaLookup mgr) {
        String table = lookupOrCreate(colFamily.getColumnFamily(), mgr);
        //Info info = fetchDbCollectionInfo(colFamily.getColumnFamily(), mgr);
        if(table == null) {
            //If there is no column family in cassandra, then we need to return no rows to the user...
            return new CursorReturnsEmptyRows2(rowKeys);
        }
        CursorKeysToRowsCql3 cursor = new CursorKeysToRowsCql3(rowKeys, batchSize, list, rowProvider, session, keys);
        cursor.setupMore(colFamily, cache);
        return cursor;
    }

    @Override
    public void readMetaAndCreateTable(MetaLookup ormSession, String colFamily) {
        // TODO Auto-generated method stub

    }

    @Override
    public AbstractCursor<Row> allRows(DboTableMeta colFamily, MetaLookup mgr, int batchSize) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getDriverHelper() {
        // TODO Auto-generated method stub
        return null;
    }

}