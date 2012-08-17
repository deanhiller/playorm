package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
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
import com.netflix.astyanax.model.ColumnList;
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
	public Iterable<KeyValue<Row>> find2(String colFamily, Iterable<byte[]> rowKeys) {
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
		OperationResult<Rows<byte[], byte[]>> result = slice.execute();
		Rows rows = result.getResult();
		
		ResultIterable r = new ResultIterable(rowProvider, rows);
		
		return r;
	}
	
	@Override	
	public List<Row> find(String colFamily, List<byte[]> keys) {
		try {
			return findImpl(colFamily, keys);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
	public List<Row> findImpl(String colFamily, List<byte[]> keys) throws ConnectionException {
		Info info = columnFamilies.fetchColumnFamilyInfo(colFamily);
		if(info == null) {
			//well, if column family doesn't exist, then no entities exist either
			log.info("query was run on column family that does not yet exist="+colFamily);
			return createEmptyList(keys);
		}

		ColumnFamily cf = info.getColumnFamilyObj();
		ColumnType type = info.getColumnType();
		if(type != ColumnType.ANY_EXCEPT_COMPOSITE) {
			throw new UnsupportedOperationException("Finding on composite type not allowed here, you should be using column slice as these rows are HUGE!!!!");
		}
		
		Keyspace keyspace = columnFamilies.getKeyspace();
		ColumnFamilyQuery<byte[], byte[]> query = keyspace.prepareQuery(cf);
		RowSliceQuery<byte[], byte[]> slice = query.getKeySlice(keys);
		OperationResult<Rows<byte[], byte[]>> result = slice.execute();
		Rows rows = result.getResult();
		
		List<Row> retVal = new ArrayList<Row>();
		for(byte[] key : keys) {
			com.netflix.astyanax.model.Row<byte[], byte[]> row = rows.getRow(key);
			if(row.getColumns().isEmpty()) {
				//Astyanax returns a row when there is none BUT we know if there are 0 columns there is really no row in the database
				//then
				retVal.add(null);
			} else {
				Row r = rowProvider.get();
				r.setKey(key);
				processColumns(row, r);
				retVal.add(r);
			}
		}
		
		return retVal;
	}

	@SuppressWarnings("unused")
	private List<Row> createEmptyList(List<byte[]> keys) {
		List<Row> rows = new ArrayList<Row>();
		for(byte[] key : keys) {
			rows.add(null);
		}
		return rows;
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
		
		m.execute();
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
			bigInt.setValue(indexedValue);
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
	public Iterable<Column> columnRangeScan(ScanInfo info, Key from, Key to, int batchSize) {
		if(batchSize <= 0)
			throw new IllegalArgumentException("batch size must be supplied and be greater than 0");
		String colFamily = info.getIndexColFamily();
		byte[] rowKey = info.getRowKey();
		Info info1 = columnFamilies.fetchColumnFamilyInfo(colFamily);
		if(info1 == null) {
			//well, if column family doesn't exist, then no entities exist either
			log.info("query was run on column family that does not yet exist="+colFamily);
			return new ArrayList<Column>();
		}
		
		ColumnType type = info1.getColumnType();
		if(type == ColumnType.ANY_EXCEPT_COMPOSITE) {
			ByteBufferRange range = new RangeBuilder().setStart(from.getKey()).setEnd(to.getKey()).setLimit(batchSize).build();
			return findBasic(rowKey, range, info1);
			
		} else if(type == ColumnType.COMPOSITE_INTEGERPREFIX ||
				type == ColumnType.COMPOSITE_DECIMALPREFIX ||
				type == ColumnType.COMPOSITE_STRINGPREFIX) {
			CompositeRangeBuilder range = setupRangeBuilder(from, to, info1);
			range = range.limit(batchSize);
			return findBasic(rowKey, range, info1);
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
	
	private Iterable<Column> findBasic(byte[] rowKey, ByteBufferRange range, Info info) {
		ColumnFamily cf = info.getColumnFamilyObj();
		
		Keyspace keyspace = columnFamilies.getKeyspace();
		ColumnFamilyQuery query = keyspace.prepareQuery(cf);
		RowQuery rowQuery = query.getKey(rowKey)
							.autoPaginate(true)
							.withColumnRange(range);
		
		return new OurIter(cf, rowQuery, info);
	}
	
	private class OurIter implements Iterable<Column> {

		private ColumnFamily cf;
		private RowQuery query;
		private Info info;

		public OurIter(ColumnFamily cf, RowQuery query2, Info info) {
			this.cf = cf;
			this.query = query2;
			this.info = info;
		}

		@Override
		public Iterator<Column> iterator() {
			return new OurIterator(cf, query, info);
		}
	}
	private class OurIterator implements Iterator<Column> {
		private RowQuery<byte[], byte[]> query;
		private Iterator<com.netflix.astyanax.model.Column<byte[]>> subIterator;
		private Info info;
		
		public OurIterator(ColumnFamily cf, RowQuery query, Info info) {
			this.query = query;
			this.info = info;
		}

		@Override
		public boolean hasNext() {
			fetchMoreResults();
			if(subIterator == null)
				return false;
			
			return subIterator.hasNext();
		}

		@Override
		public Column next() {
			fetchMoreResults();
			if(subIterator == null)
				throw new ArrayIndexOutOfBoundsException("no more elements");

			com.netflix.astyanax.model.Column col = subIterator.next();

			Object obj = col.getName();
			byte[] name;
			switch (info.getColumnType()) {
			case ANY_EXCEPT_COMPOSITE:
				name = (byte[])obj;
				break;
			case COMPOSITE_STRINGPREFIX:
			case COMPOSITE_DECIMALPREFIX:
			case COMPOSITE_INTEGERPREFIX:
				GenericComposite bigDec = (GenericComposite)obj;
				name = bigDec.getPk();
				break;
			default:
				throw new UnsupportedOperationException("type not supported yet="+info.getColumnType());
			}
			
			Column c = new Column();
			c.setName(name);
			c.setValue(col.getByteArrayValue());
			c.setTimestamp(col.getTimestamp());
			
			return c;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported");
		}
		
		
		private void fetchMoreResults() {
			try {
				fetchMoreResultsImpl();
			} catch (ConnectionException e) {
				throw new RuntimeException(e);
			}
		}
		
		private void fetchMoreResultsImpl() throws ConnectionException {
			if(subIterator != null && subIterator.hasNext())
				return; //no need to fetch more since subIterator has more
			
			ColumnList<byte[]> columns = query.execute().getResult();
			if(columns.isEmpty())
				subIterator = null; 
			else 
				subIterator = columns.iterator();
		}
	}

}
