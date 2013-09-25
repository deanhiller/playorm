package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

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
import com.datastax.driver.core.Session;

public class CqlSession implements NoSqlRawSession {
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

    private String lookupOrCreate(String colFamily1, MetaLookup ormSession) {
        if (cluster.getMetadata().getKeyspace(keys).getTable(colFamily1.toLowerCase()) == null) {
            try {
                if (colFamily1.equalsIgnoreCase("StringIndice")) {
                    session.execute("CREATE TABLE " + keys + "." + colFamily1 + " (id text," + "colname text," + "colvalue blob,"
                            + "PRIMARY KEY (id,colname, colvalue)" + ") WITH COMPACT STORAGE");
                } else if (colFamily1.equalsIgnoreCase("IntegerIndice")) {
                    session.execute("CREATE TABLE " + keys + "." + colFamily1 + " (id text," + "colname bigint," + "colvalue blob,"
                            + "PRIMARY KEY (id,colname, colvalue)" + ") WITH COMPACT STORAGE");
                } else if (colFamily1.equalsIgnoreCase("DecimalIndice")) {
                    session.execute("CREATE TABLE " + keys + "." + colFamily1 + " (id text," + "colname float," + "colvalue blob,"
                            + "PRIMARY KEY (id,colname, colvalue)" + ") WITH COMPACT STORAGE");
                } else {
                    session.execute("CREATE TABLE " + keys + "." + colFamily1
                            + " (id text, colname text, colvalue blob, PRIMARY KEY (id,colname, colvalue)) WITH COMPACT STORAGE");
                }

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
        CursorForValues cursor = new CursorForValues(scanInfo, list,
                values, session, keys);
        return cursor;
    }

    @Override
    public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily, DirectCursor<byte[]> rowKeys, Cache cache, int batchSize, BatchListener list,
            MetaLookup mgr) {
        String table = lookupOrCreate(colFamily.getColumnFamily(), mgr);
        //Info info = fetchDbCollectionInfo(colFamily.getColumnFamily(), mgr);
        if(table == null) {
            //If there is no column family in mongodb, then we need to return no rows to the user...
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