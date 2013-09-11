package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.ColumnSliceInfo;
import com.alvazan.orm.api.z8spi.ColumnType;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.Action;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.action.Persist;
import com.alvazan.orm.api.z8spi.action.PersistIndex;
import com.alvazan.orm.api.z8spi.action.Remove;
import com.alvazan.orm.api.z8spi.action.RemoveColumn;
import com.alvazan.orm.api.z8spi.action.RemoveIndex;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.iter.EmptyCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnToManyMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.TokenRangeOfflineException;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowQuery;
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
		columnFamilies.close();
	}
	
	@Override
	public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily,
			DirectCursor<byte[]> rowKeys, Cache cache, int batchSize, BatchListener list, MetaLookup mgr) {
		Info info = columnFamilies.fetchColumnFamilyInfo(colFamily.getColumnFamily(), mgr);
		if(info == null) {
			//If there is no column family in cassandra, then we need to return no rows to the user...
			return new CursorReturnsEmptyRows(rowKeys);
		}
		
		ColumnType type = info.getColumnType();
		if(type != ColumnType.ANY_EXCEPT_COMPOSITE) {
			throw new UnsupportedOperationException("Finding on composite type="+colFamily+" not allowed here, you should be using column slice as these rows are HUGE!!!!");
		}
		
		Keyspace keyspace = columnFamilies.getKeyspace();
		CursorKeysToRows2 cursor = new CursorKeysToRows2(rowKeys, batchSize, list, rowProvider);
		cursor.setupMore(keyspace, colFamily, info, cache);
		return cursor;
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
			c.setTtl(col.getTtl());

			r.put(c);
		}
	}

	@Override
	public void sendChanges(List<Action> actions, MetaLookup ormSession) {
		try {
			sendChangesImpl(actions, ormSession);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	public void sendChangesImpl(List<Action> actions, MetaLookup ormSession) throws ConnectionException {
		Keyspace keyspace = columnFamilies.getKeyspace();
		MutationBatch m = keyspace.prepareMutationBatch();
		//MutationBatch m = m1.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
		
		for(Action action : actions) {
			if(action instanceof Persist) {
				persist((Persist)action, ormSession, m);
			} else if(action instanceof Remove) {
				remove((Remove)action, ormSession, m);
			} else if(action instanceof PersistIndex) {
				persistIndex((PersistIndex)action, ormSession, m);
			} else if(action instanceof RemoveIndex) {
				removeIndex((RemoveIndex)action, ormSession, m);
			} else if(action instanceof RemoveColumn) {
				removeColumn((RemoveColumn) action, ormSession, m);
			}
		}
		
		long time = System.currentTimeMillis();
        try {
            m.execute();
        } catch (TokenRangeOfflineException e) {
            throw new RuntimeException(
                    "It appears your CL_LEVEL is CL_QUOROM and there are not enough nodes on line to service that request.  Either lower your CL_LEVEL or make sure all nodes are operational");
        }
		
		if(log.isTraceEnabled()) {
			long total = System.currentTimeMillis()-time;
			log.trace("astyanx save took="+total+" ms");
		}
	}

	
	private void remove(Remove action, MetaLookup mgr, MutationBatch m) {
		Info info = columnFamilies.fetchColumnFamilyInfo(action.getColFamily().getColumnFamily(), mgr);
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

	private void removeColumn(RemoveColumn action, MetaLookup mgr, MutationBatch m) {
		Info info = columnFamilies.fetchColumnFamilyInfo(action.getColFamily().getColumnFamily(), mgr);
		if(info == null)
			return; //if no cf exist/returned, nothing to do
		ColumnFamily cf = info.getColumnFamilyObj();
		ColumnListMutation row = m.withRow(cf, action.getRowKey());
		if(row == null)
			return;
		byte[] name = action.getColumn();
			row.deleteColumn(name);
	}

	private void persistIndex(PersistIndex action, MetaLookup mgr, MutationBatch m) {
		String indexCfName = action.getIndexCfName();
		Info info = columnFamilies.lookupOrCreate2(indexCfName, mgr);

		ColumnFamily cf = info.getColumnFamilyObj();
		ColumnListMutation colMutation = m.withRow(cf, action.getRowKey());
		Object toPersist = createObjectToUse(action, info);
		colMutation.putEmptyColumn(toPersist, action.getRowTtl());
	}

	private void removeIndex(RemoveIndex action, MetaLookup mgr, MutationBatch m) {
		String indexCfName = action.getIndexCfName();
		if (indexCfName.equalsIgnoreCase("BytesIndice"))
			return;
		Info info = columnFamilies.fetchColumnFamilyInfo(indexCfName, mgr);
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
	
	private void persist(Persist action, MetaLookup ormSession, MutationBatch m) {
		Info info = columnFamilies.lookupOrCreate2(action.getColFamily().getColumnFamily(), ormSession);
		ColumnFamily cf = info.getColumnFamilyObj();
		
		ColumnListMutation colMutation = m.withRow(cf, action.getRowKey());
		
		for(Column col : action.getColumns()) {
			byte[] value = new byte[0];
			if(col.getValue() != null)
				value = col.getValue();

			colMutation.putColumn(col.getName(), value, col.getTtl());
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
		if (log.isInfoEnabled())
			log.info("Clearing keyspace="+keyspaceName+" in cassandra");
		List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
		KeyspaceDefinition ourDef = null;
		for(KeyspaceDefinition kDef : keyspaces) {
			if(keyspaceName.equalsIgnoreCase(kDef.getName())) {
				ourDef = kDef;
				break;
			}
		}
		
		cluster.dropKeyspace(keyspaceName);
		OperationResult<SchemaChangeResult> result = cluster.addKeyspace(ourDef);
		
		columnFamilies.waitForNodesToBeUpToDate(result, 300000);
	}



	@Override
	public AbstractCursor<Column> columnSlice(ColumnSliceInfo sliceInfo, final Integer batchSize, BatchListener batchListener, MetaLookup mgr) {
		if(batchSize <= 0)
			throw new IllegalArgumentException("batch size must be supplied and be greater than 0");
		final Info info1 = columnFamilies.fetchColumnFamilyInfo(sliceInfo.getColFamily().getColumnFamily(), mgr);
		if(info1 == null) {
			//well, if column family doesn't exist, then no entities exist either
			if (log.isInfoEnabled())
				log.info("query was run on column family that does not yet exist="+sliceInfo.getColFamily());
			return new EmptyCursor<Column>();
		}
		final byte[] rowKey = sliceInfo.getRowKey();
		final byte[] from = sliceInfo.getFrom();
		final byte[] to = sliceInfo.getTo();

		CreateColumnSliceCallback l = new CreateColumnSliceCallback() {
			
			@Override
			public RowQuery<byte[], byte[]> createRowQuery() {
				RangeBuilder rangeBldr = new RangeBuilder();
				if(from != null)
					rangeBldr.setStart(from);
				if(to != null)
					rangeBldr.setEnd(to);
				if(batchSize != null)
					rangeBldr = rangeBldr.setLimit(batchSize);
				ByteBufferRange range = rangeBldr.build(); 
				return createBasicRowQuery(rowKey, info1, range);
			}

			@Override
			public RowQuery<byte[], byte[]> createRowQueryReverse() {
				RangeBuilder rangeBldr = new RangeBuilder();
				if(to != null)
					rangeBldr.setStart(to);
				if(from != null)
					rangeBldr.setEnd(from);
					
				rangeBldr.setReversed(true);
				
				if(batchSize != null)
					rangeBldr = rangeBldr.setLimit(batchSize);
				ByteBufferRange range = rangeBldr.build(); 
				return createBasicRowQuery(rowKey, info1, range);
			}
			
			
		};
			

		return findBasic(Column.class, rowKey, l, batchListener, batchSize, "general column slice", null);
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo info, List<byte[]> values, BatchListener batchList, MetaLookup mgr) {
		return scanIndex(info, values, batchList, mgr, false);
	}
	
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo info, List<byte[]> values, BatchListener batchList, MetaLookup mgr, boolean reverse) {
		String colFamily = info.getIndexColFamily();
		Info info1 = columnFamilies.fetchColumnFamilyInfo(colFamily, mgr);
		if(info1 == null) {
			//well, if column family doesn't exist, then no entities exist either
			if (log.isInfoEnabled())
				log.info("query was run on column family that does not yet exist="+colFamily);
			return new EmptyCursor<IndexColumn>();
		}
		
		ColumnType type = info1.getColumnType();
		if(type == ColumnType.COMPOSITE_INTEGERPREFIX ||
				type == ColumnType.COMPOSITE_DECIMALPREFIX ||
				type == ColumnType.COMPOSITE_STRINGPREFIX) {
			StartQueryListener listener = new StartQueryManyKeys(columnFamilies, info1, info, values, reverse);
			return new CursorOfFutures(listener, batchList);
		} else
			throw new UnsupportedOperationException("not done here yet");
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo info, Key from, Key to,
			Integer batchSize, BatchListener bListener, MetaLookup mgr) {
		if(batchSize != null && batchSize <= 0)
			throw new IllegalArgumentException("batch size must be supplied and be greater than 0");
		String colFamily = info.getIndexColFamily();
		byte[] rowKey = info.getRowKey();

		//Here we don't bother using an index at all since there is no where clause to begin with
        //ALSO, we don't want this code to be the case if we are doing a CursorToMany which has to use an index
        //so check the column type
        if(!info.getEntityColFamily().isVirtualCf() && from == null && to == null
                && !(info.getColumnName() instanceof DboColumnToManyMeta)
                && !info.getEntityColFamily().isInheritance()) {
            Keyspace keyspace = columnFamilies.getKeyspace();
            Info cfInfo = columnFamilies.lookupOrCreate2(info.getEntityColFamily().getRealColumnFamily(), mgr);
            ScanCassandraCf scanner = new ScanCassandraCf(info, cfInfo, bListener, batchSize, keyspace);
            scanner.beforeFirst();
            return scanner;
        }

        Info info1 = columnFamilies.fetchColumnFamilyInfo(colFamily, mgr);
		if(info1 == null) {
			//well, if column family doesn't exist, then no entities exist either
			if (log.isInfoEnabled())
				log.info("query was run on column family that does not yet exist="+colFamily);
			return new EmptyCursor<IndexColumn>();
		}
		

		
		String colName = null;
		if(info.getColumnName() != null)
			colName = info.getColumnName().getColumnName();
		
		ColumnType type = info1.getColumnType();
		if(type == ColumnType.COMPOSITE_INTEGERPREFIX ||
				type == ColumnType.COMPOSITE_DECIMALPREFIX ||
				type == ColumnType.COMPOSITE_STRINGPREFIX) {
			Listener l = new Listener(rowKey, info1, from, to, batchSize);
			return findBasic(IndexColumn.class, rowKey, l, bListener, batchSize, ""+info, colName);
		} else
			throw new UnsupportedOperationException("not done here yet");
	}
	
	public static CompositeRangeBuilder setupRangeBuilder(Key from, Key to, Info info1, boolean reverse) {
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
		if (reverse)
			range = range.reverse();
		return range;
	}
	
	private RowQuery createBasicRowQuery(byte[] rowKey, Info info1, ByteBufferRange range) {
		ColumnFamily cf = info1.getColumnFamilyObj();
		
		Keyspace keyspace = columnFamilies.getKeyspace();
		ColumnFamilyQuery query = keyspace.prepareQuery(cf);
		//ColumnFamilyQuery query = query1.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
		RowQuery rowQuery = query.getKey(rowKey)
							.autoPaginate(true)
							.withColumnRange(range);
		return rowQuery;
	}

	private <T> AbstractCursor<T> findBasic(Class<T> clazz, byte[] rowKey, CreateColumnSliceCallback l, BatchListener bListener, Integer batchSize, String logInfo, String colName) {
		boolean isComposite = IndexColumn.class == clazz;
		return new CursorColumnSlice<T>(l, isComposite, bListener, batchSize, logInfo, colName);
	}
	
	public interface CreateColumnSliceCallback {
		RowQuery<byte[], byte[]> createRowQuery();
		RowQuery<byte[], byte[]> createRowQueryReverse();
	}
	
	private class Listener implements CreateColumnSliceCallback {
		private byte[] rowKey;
		private Info info1;
		private Key from;
		private Key to;
		private Integer batchSize;

		public Listener(byte[] rowKey, Info info1, Key from, Key to, Integer batchSize) {
			this.rowKey = rowKey;
			this.info1 = info1;
			this.from = from;
			this.to = to;
			this.batchSize = batchSize;
		}

		/**
		 * For some dang reason with astyanax, we have to recreate the row query from scratch before we re-use it for 
		 * a NEsted join.
		 * @return
		 */
		public RowQuery<byte[], byte[]> createRowQuery() {
			CompositeRangeBuilder range = setupRangeBuilder(from, to, info1, false);
			if(batchSize != null)
				range = range.limit(batchSize);			
			return createBasicRowQuery(rowKey, info1, range);
		}
		
		/**
		 * For some dang reason with astyanax, we have to recreate the row query from scratch before we re-use it for 
		 * a NEsted join.
		 * @return
		 */
		public RowQuery<byte[], byte[]> createRowQueryReverse() {
			CompositeRangeBuilder range = setupRangeBuilder(from, to, info1, true);
			if(batchSize != null)
				range = range.limit(batchSize);			
			return createBasicRowQuery(rowKey, info1, range);
		}
	}

	@Override
	public void readMetaAndCreateTable(MetaLookup ormSession, String colFamily) {
		if(log.isDebugEnabled()) {
			log.debug("readMeta and create CF="+colFamily);
		}
		columnFamilies.lookupOrCreate2(colFamily, ormSession);
	}

	@Override
	public AbstractCursor<Row> allRows(DboTableMeta colFamily, MetaLookup mgr, int batchSize) {
		Keyspace keyspace = columnFamilies.getKeyspace();
		Info cfInfo = columnFamilies.lookupOrCreate2(colFamily.getColumnFamily(), mgr);
		ScanCassandraCfAllRows scanner = new ScanCassandraCfAllRows(cfInfo, batchSize, keyspace, rowProvider);
		scanner.beforeFirst();
		return scanner;
	}

	@Override
	public Object getDriverHelper() {
		return columnFamilies;
	}

}
