package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi9.db.Action;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.ColumnType;
import com.alvazan.orm.api.spi9.db.IndexColumn;
import com.alvazan.orm.api.spi9.db.Key;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;
import com.alvazan.orm.api.spi9.db.Persist;
import com.alvazan.orm.api.spi9.db.PersistIndex;
import com.alvazan.orm.api.spi9.db.Remove;
import com.alvazan.orm.api.spi9.db.RemoveIndex;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.api.spi9.db.ScanInfo;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.CompositeRangeBuilder;
import com.netflix.astyanax.util.RangeBuilder;

@SuppressWarnings({"unchecked", "rawtypes"})
public class CassandraSession implements NoSqlRawSession {

	private static final Logger log = LoggerFactory.getLogger(CassandraSession.class);
	
	@Inject
	private ColumnFamilyHelper columnFamilies;
	@Inject
	private Provider<Row> rowProvider;
	
	@Override
	public void start(Map<String, Object> properties) {
		try {
			columnFamilies.start(properties);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void close() {
		throw new UnsupportedOperationException("not done here yet");
	}
	
	@Override
	public Iterable<KeyValue<Row>> find(String colFamily, Iterable<byte[]> rowKeys) {
		try {
			return findImpl2(colFamily, rowKeys);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}		
	}
	
	public Iterable<KeyValue<Row>> findImpl2(String colFamily, Iterable<byte[]> keys) throws ConnectionException {
		Info info = columnFamilies.fetchColumnFamilyInfo(colFamily);
		if(info == null) {
			//well, if column family doesn't exist, then no entities exist either
			log.info("query was run on column family that does not yet exist="+colFamily);
			return new IterEmptyProxy(keys);
		}

		ColumnFamily cf = info.getColumnFamilyObj();
		ColumnType type = info.getColumnType();
		if(type != ColumnType.ANY_EXCEPT_COMPOSITE) {
			throw new UnsupportedOperationException("Finding on composite type not allowed here, you should be using column slice as these rows are HUGE!!!!");
		}
		
		Keyspace keyspace = columnFamilies.getKeyspace();
		ColumnFamilyQuery<byte[], byte[]> query = keyspace.prepareQuery(cf);
		RowSliceQuery<byte[], byte[]> slice = query.getKeySlice(keys);
		
		long time = System.currentTimeMillis();
		OperationResult<Rows<byte[], byte[]>> result = slice.execute();
		if(log.isInfoEnabled()) {
			long total = System.currentTimeMillis()-time;
			log.info("astyanx find took="+total+" ms");
		}
		
		Rows rows = result.getResult();
		
		ResultIterable r = new ResultIterable(rowProvider, rows);
		
		return r;
	}

	static void processColumns(
			com.netflix.astyanax.model.Row<byte[], byte[]> row, Row r) {
		for(com.netflix.astyanax.model.Column<byte[]> col : row.getColumns()) {
			byte[] name = col.getName();
			byte[] val = col.getByteArrayValue();
			long timestamp = col.getTimestamp();
			Column c = new Column();
			c.setName(name);
			if(val.length != 0)
				c.setValue(val);
			c.setTimestamp(timestamp);
			
			r.put(name, c);
		}
	}

	@Override
	public void sendChanges(List<Action> actions, Object ormSession) {
		try {
			sendChangesImpl(actions, (NoSqlEntityManager) ormSession);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	public void sendChangesImpl(List<Action> actions, NoSqlEntityManager mgr) throws ConnectionException {
		Keyspace keyspace = columnFamilies.getKeyspace();
		MutationBatch m = keyspace.prepareMutationBatch();
		for(Action action : actions) {
			if(action instanceof Persist) {
				persist((Persist)action, mgr, m);
			} else if(action instanceof Remove) {
				remove((Remove)action, m);
			} else if(action instanceof PersistIndex) {
				persistIndex((PersistIndex)action, mgr, m);
			} else if(action instanceof RemoveIndex) {
				removeIndex((RemoveIndex)action, m);
			}
		}
		
		long time = System.currentTimeMillis();
		m.execute();
		
		if(log.isInfoEnabled()) {
			long total = System.currentTimeMillis()-time;
			log.info("astyanx save took="+total+" ms");
		}
	}

	
	private void remove(Remove action, MutationBatch m) {
		Info info = columnFamilies.fetchColumnFamilyInfo(action.getColFamily());
		if(info == null)
			return; //if no cf exist/returned, nothing to do
		ColumnFamily cf = info.getColumnFamilyObj();
		
		switch(action.getAction()) {
		case REMOVE_ENTIRE_ROW:
			m.withRow(cf, action.getRowKey()).delete();
			break;
		case REMOVE_COLUMNS_FROM_ROW:
			removeColumns(action, cf, m);
			break;
		default:
			throw new RuntimeException("bug, unknown remove action="+action.getAction());
		}
	}

	private void removeColumns(Remove action, ColumnFamily cf, MutationBatch m) {
		ColumnListMutation row = m.withRow(cf, action.getRowKey());
		
		for(byte[] name : action.getColumns()) {
			row.deleteColumn(name);
		}
	}

	private void persistIndex(PersistIndex action, NoSqlEntityManager mgr, MutationBatch m) {
		Info info = columnFamilies.lookupOrCreate2(action.getColFamily(), mgr);

		ColumnFamily cf = info.getColumnFamilyObj();
		ColumnListMutation colMutation = m.withRow(cf, action.getRowKey());
		Object toPersist = createObjectToUse(action, info);
		
		colMutation.putEmptyColumn(toPersist);
	}

	private void removeIndex(RemoveIndex action, MutationBatch m) {
		Info info = columnFamilies.fetchColumnFamilyInfo(action.getColFamily());
		if(info == null)
			return; //nothing to do since it doesn't exist
		
		ColumnFamily cf = info.getColumnFamilyObj();
		ColumnListMutation colMutation = m.withRow(cf, action.getRowKey());
		Object toRemove = createObjectToUse(action, info);
		
		colMutation.deleteColumn(toRemove);
	}
	
	private Object createObjectToUse(RemoveIndex action, Info info) {
		byte[] indexedValue = action.getColumn().getIndexedValue();
		byte[] pk = action.getColumn().getPrimaryKey();
		
		ColumnType type = info.getColumnType();
		Object toPersist;
		switch(type) {
		case COMPOSITE_STRINGPREFIX:
		case COMPOSITE_INTEGERPREFIX:
		case COMPOSITE_DECIMALPREFIX:
			GenericComposite bigInt = new GenericComposite();
			bigInt.setIndexedValue(indexedValue);
			bigInt.setPk(pk);
			toPersist = bigInt;
			break;
		default:
			throw new UnsupportedOperationException("not supported at this time. type="+type);
		}
		return toPersist;
	}
	
	private void persist(Persist action, NoSqlEntityManager mgr, MutationBatch m) {
		
		Info info = columnFamilies.lookupOrCreate2(action.getColFamily(), mgr);
		ColumnFamily cf = info.getColumnFamilyObj();
		
		ColumnListMutation colMutation = m.withRow(cf, action.getRowKey());
		
		for(Column col : action.getColumns()) {
			Integer theTime = null;
			Long time = col.getTimestamp();
			if(time != null)
				theTime = (int)time.longValue();
			byte[] value = new byte[0];
			if(col.getValue() != null)
				value = col.getValue();
			
			colMutation.putColumn(col.getName(), value, theTime);
		}
	}

	@Override
	public void clearDatabase() {
		try {
			clearImpl();
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	public void clearImpl() throws ConnectionException {
		Cluster cluster = columnFamilies.getCluster();
		String keyspaceName = columnFamilies.getKeyspaceName();
		List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
		KeyspaceDefinition ourDef = null;
		for(KeyspaceDefinition kDef : keyspaces) {
			if(keyspaceName.equalsIgnoreCase(kDef.getName())) {
				ourDef = kDef;
				break;
			}
		}
		
		cluster.dropKeyspace(keyspaceName);
		cluster.addKeyspace(ourDef);
	}

	@Override
	public Iterable<Column> columnSlice(String colFamily, byte[] rowKey, byte[] from, byte[] to, int batchSize) {
		if(batchSize <= 0)
			throw new IllegalArgumentException("batch size must be supplied and be greater than 0");
		Info info1 = columnFamilies.fetchColumnFamilyInfo(colFamily);
		if(info1 == null) {
			//well, if column family doesn't exist, then no entities exist either
			log.info("query was run on column family that does not yet exist="+colFamily);
			return new ArrayList<Column>();
		}
		
		ByteBufferRange range = new RangeBuilder().setStart(from).setEnd(to).setLimit(batchSize).build();
		return findBasic(Column.class, rowKey, range, info1, batchSize);
	}

	@Override
	public Iterable<IndexColumn> scanIndex(ScanInfo info, Key from, Key to,
			int batchSize) {
		if(batchSize <= 0)
			throw new IllegalArgumentException("batch size must be supplied and be greater than 0");
		String colFamily = info.getIndexColFamily();
		byte[] rowKey = info.getRowKey();
		Info info1 = columnFamilies.fetchColumnFamilyInfo(colFamily);
		if(info1 == null) {
			//well, if column family doesn't exist, then no entities exist either
			log.info("query was run on column family that does not yet exist="+colFamily);
			return new ArrayList<IndexColumn>();
		}
		
		ColumnType type = info1.getColumnType();
		if(type == ColumnType.COMPOSITE_INTEGERPREFIX ||
				type == ColumnType.COMPOSITE_DECIMALPREFIX ||
				type == ColumnType.COMPOSITE_STRINGPREFIX) {
			CompositeRangeBuilder range = setupRangeBuilder(from, to, info1);
			range = range.limit(batchSize);
			return findBasic(IndexColumn.class, rowKey, range, info1, batchSize);
		} else
			throw new UnsupportedOperationException("not done here yet");
	}
	
	private CompositeRangeBuilder setupRangeBuilder(Key from, Key to, Info info1) {
		AnnotatedCompositeSerializer serializer = info1.getCompositeSerializer();
		CompositeRangeBuilder range = serializer.buildRange();
		if(from != null) {
			if(from.isInclusive())
				range = range.greaterThanEquals(from.getKey());
			else
				range = range.greaterThan(from.getKey());
		}
		if(to != null) {
			if(to.isInclusive())
				range = range.lessThanEquals(to.getKey());
			else
				range = range.lessThan(to.getKey());
		}
		return range;
	}
	
	private <T> Iterable<T> findBasic(Class<T> clazz, byte[] rowKey, ByteBufferRange range, Info info, int batchSize) {
		ColumnFamily cf = info.getColumnFamilyObj();
		
		Keyspace keyspace = columnFamilies.getKeyspace();
		ColumnFamilyQuery query = keyspace.prepareQuery(cf);
		RowQuery rowQuery = query.getKey(rowKey)
							.autoPaginate(true)
							.withColumnRange(range);
		
		boolean isComposite = IndexColumn.class == clazz;
		return new OurIter<T>(rowQuery, batchSize, isComposite);
	}
	
	

}
