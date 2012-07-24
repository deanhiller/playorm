package com.alvazan.orm.layer3.spi.db.cassandra;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi.db.Action;
import com.alvazan.orm.api.spi.db.Column;
import com.alvazan.orm.api.spi.db.NoSqlRawSession;
import com.alvazan.orm.api.spi.db.Persist;
import com.alvazan.orm.api.spi.db.Remove;
import com.alvazan.orm.api.spi.db.Row;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@SuppressWarnings({"unchecked", "rawtypes"})
public class CassandraSession implements NoSqlRawSession {

	private static final Logger log = LoggerFactory.getLogger(CassandraSession.class);
	private Keyspace keyspace;
	
	private Set<String> existingColumnFamilies = new HashSet<String>();
	private Cluster cluster;
	
	@Override
	public void start(Map<String, String> properties) {
		try {
			startImpl(properties);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void startImpl(Map<String, String> properties) throws ConnectionException {
		Builder builder = new AstyanaxContext.Builder()
	    .forCluster("SDICluster")
	    .forKeyspace("SDIKeyspace")
	    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
	        .setDiscoveryType(NodeDiscoveryType.NONE)
	    )
	    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
	        .setPort(9160)
	        .setMaxConnsPerHost(1)
	        .setSeeds("127.0.0.1:9160")
	    )
	    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());
		
		AstyanaxContext<Keyspace> context = builder.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		
		AstyanaxContext<Cluster> clusterContext = builder.buildCluster(ThriftFamilyFactory.getInstance());
		clusterContext.start();
		
		keyspace = context.getEntity();
		cluster = clusterContext.getEntity();
		KeyspaceDefinition keySpaceMeta = keyspace.describeKeyspace();
		
		List<ColumnFamilyDefinition> cfList = keySpaceMeta.getColumnFamilyList();
		for(ColumnFamilyDefinition def : cfList) {
			existingColumnFamilies.add(def.getName());
		}
		log.info("Existing column families="+existingColumnFamilies+"\nNOTE: WE WILL CREATE " +
				"new column families automatically as you save entites that have no column family");
	}



	@Override
	public void stop() {

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
		if(!existingColumnFamilies.contains(colFamily)) {
			//well, if column family doesn't exist, then no entities exist either
			log.info("query was run on column family that does not yet exist="+colFamily);
			return createEmptyList(keys);
		}
		
		ColumnFamily<byte[], byte[]> cf = lookupOrCreate(colFamily);
		ColumnFamilyQuery<byte[], byte[]> query = keyspace.prepareQuery(cf);
		RowSliceQuery<byte[], byte[]> slice = query.getKeySlice(keys);
		OperationResult<Rows<byte[], byte[]>> result = slice.execute();
		Rows<byte[], byte[]> rows = result.getResult();
		
		List<Row> retVal = new ArrayList<Row>();
		for(byte[] key : keys) {
			com.netflix.astyanax.model.Row<byte[], byte[]> row = rows.getRow(key);
			if(row.getColumns().isEmpty()) {
				//Astyanax returns a row when there is none BUT we know if there are 0 columns there is really no row in the database
				//then
				retVal.add(null);
			} else {
				Row r = new Row();
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

	private void processColumns(
			com.netflix.astyanax.model.Row<byte[], byte[]> row, Row r) {
		for(com.netflix.astyanax.model.Column<byte[]> col : row.getColumns()) {
			byte[] name = col.getName();
			byte[] val = col.getByteArrayValue();
			long timestamp = col.getTimestamp();
			Column c = new Column();
			c.setName(name);
			c.setValue(val);
			c.setTimestamp(timestamp);
			
			r.put(name, c);
		}
	}

	@Override
	public void sendChanges(List<Action> actions) {
		try {
			sendChangesImpl(actions);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	public void sendChangesImpl(List<Action> actions) throws ConnectionException {
		MutationBatch m = keyspace.prepareMutationBatch();
		for(Action action : actions) {
			ColumnFamily cf = lookupOrCreate(action.getColFamily());
			
			if(action instanceof Persist) {
				persist((Persist)action, cf, m);
			} else if(action instanceof Remove) {
				remove((Remove)action, cf, m);
			}
		}
		
		m.execute();
	}

	private ColumnFamily lookupOrCreate(String colFamily) throws ConnectionException {
		if(!existingColumnFamilies.contains(colFamily)) {
			log.info("CREATING column family="+colFamily+" in cassandra");
			cluster.addColumnFamily(cluster.makeColumnFamilyDefinition()
				    .setName(colFamily)
				    .setKeyspace(keyspace.getKeyspaceName())
				);
			existingColumnFamilies.add(colFamily);
		}
		
		
		//should we cache this and just look it up each time or is KISS fine for now....
		ColumnFamily cf = new ColumnFamily(colFamily, BytesArraySerializer.get(), BytesArraySerializer.get());
		return cf;
	}

	private void remove(Remove action, ColumnFamily cf, MutationBatch m) {
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

	private void persist(Persist action, ColumnFamily cf, MutationBatch m) {
		ColumnListMutation colMutation = m.withRow(cf, action.getRowKey());
		
		for(Column col : action.getColumns()) {
			//we don't need to store null values.  In fact, cassandra does NOT allow it
			if(col.getValue() == null)
				continue;
			
			Integer theTime = null;
			Long time = col.getTimestamp();
			if(time != null)
				theTime = (int)time.longValue();
			
			colMutation.putColumn(col.getName(), col.getValue(), theTime);
		}
	}

	@Override
	public void clearDatabaseIfInMemoryType() {
		try {
			clearImpl();
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	public void clearImpl() throws ConnectionException {
		for(String cf : existingColumnFamilies) {
			ColumnFamily colFamily = lookupOrCreate(cf);
			keyspace.truncateColumnFamily(colFamily); 
		}
	}


}
