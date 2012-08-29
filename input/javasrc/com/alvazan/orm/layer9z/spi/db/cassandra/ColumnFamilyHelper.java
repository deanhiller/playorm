package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi3.meta.StorageTypeEnum;
import com.alvazan.orm.api.spi9.db.ColumnType;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@SuppressWarnings("rawtypes")
public class ColumnFamilyHelper {
	private static final Logger log = LoggerFactory.getLogger(ColumnFamilyHelper.class);
	
	@Inject
	private DboDatabaseMeta dbMetaFromOrmOnly;
	
	private Map<String, Info> existingColumnFamilies2 = new HashMap<String, Info>();

	private Keyspace keyspace;
	private Cluster cluster;
	private String keyspaceName;
	
	public Keyspace getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(Keyspace keyspace) {
		this.keyspace = keyspace;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public String getKeyspaceName() {
		return keyspaceName;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public void start(Map<String, Object> properties) throws ConnectionException {
//		Object seedsObj = properties.get(Bootstrap.SEEDS);
//		Object keyspaceNameObj = properties.get(Bootstrap.KEYSPACE);
//		Object clusterNameObj = properties.get(Bootstrap.CLUSTER_NAME);
		Object builderObj = properties.get(Bootstrap.CASSANDRA_BUILDER);
//		if(seedsObj == null || !(seedsObj instanceof String))
//			throw new IllegalArgumentException("The property Bootstrap.HOST was not in the Map or was in the Map but not as a String");
//		else if(keyspaceNameObj == null || !(keyspaceNameObj instanceof String))
//			throw new IllegalArgumentException("The property Bootstrap.KEYSPACE was not in the Map or was in the Map but not as a String");
//		else if(clusterNameObj == null || !(clusterNameObj instanceof String))
//			throw new IllegalArgumentException("The property Bootstrap.CLUSTER_NAME was not in the Map or was in the Map but not as a String");
		if(builderObj == null || !(builderObj instanceof Builder))
			throw new IllegalArgumentException("The property Bootstrap.CASSANDRA_BUILDER was not in the Map or was in Map but was not of type Builder and must be supplied when using Cassandra plugin");

		Builder builder = (Builder) builderObj;
		AstyanaxContext<Cluster> clusterContext = builder.buildCluster(ThriftFamilyFactory.getInstance());
		keyspaceName = clusterContext.getKeyspaceName();
		if(keyspaceName == null)
			throw new IllegalArgumentException("You did not call Builder.forKeyspace on the astyanax Builder api.  We need to know the keyspace to continue");
		
		clusterContext.start();
		
		cluster = clusterContext.getEntity();
		List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
		boolean exists = false;
		for(KeyspaceDefinition kDef : keyspaces) {
			if(keyspaceName.equalsIgnoreCase(kDef.getName())) {
				exists = true;
				break;
			}
		}

		if(!exists) {
			KeyspaceDefinition def = cluster.makeKeyspaceDefinition();
			def.setName(keyspaceName);
			def.setStrategyClass("org.apache.cassandra.locator.SimpleStrategy");
			Map<String, String> map = new HashMap<String, String>();
			map.put("replication_factor", "3");
			def.setStrategyOptions(map);
			cluster.addKeyspace(def);
		}
		
		AstyanaxContext<Keyspace> context = builder.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		
		keyspace = context.getEntity();

		KeyspaceDefinition keySpaceMeta = keyspace.describeKeyspace();
		
		findExistingColumnFamilies(keySpaceMeta);
		log.info("Existing column families="+existingColumnFamilies2.keySet()+"\nNOTE: WE WILL CREATE " +
				"new column families automatically as you save entites that have no column family");
	}

	private void findExistingColumnFamilies(KeyspaceDefinition keySpaceMeta) {
		List<ColumnFamilyDefinition> cfList = keySpaceMeta.getColumnFamilyList();
		for(ColumnFamilyDefinition def : cfList) {
			loadColumnFamily(def);
		}
	}

	private void loadColumnFamily(ColumnFamilyDefinition def) {
		String comparatorType = def.getComparatorType();
		ColumnType type = ColumnType.ANY_EXCEPT_COMPOSITE;
		if(formName(UTF8Type.class, BytesType.class).equals(comparatorType)) {
			type = ColumnType.COMPOSITE_STRINGPREFIX;
		} else if(formName(IntegerType.class, BytesType.class).equals(comparatorType)) {
			type = ColumnType.COMPOSITE_INTEGERPREFIX;
		} else if(formName(DecimalType.class, BytesType.class).equals(comparatorType)) {
			type = ColumnType.COMPOSITE_DECIMALPREFIX;
		}
		
		String keyValidationClass = def.getKeyValidationClass();
		StorageTypeEnum keyType = null;
		if(UTF8Type.class.getName().equals(keyValidationClass)) {
			keyType = StorageTypeEnum.STRING;
		} else if(DecimalType.class.getName().equals(keyValidationClass)) {
			keyType = StorageTypeEnum.DECIMAL;
		} else if(IntegerType.class.getName().equals(keyValidationClass)) {
			keyType = StorageTypeEnum.INTEGER;
		} else if(BytesType.class.getName().equals(keyValidationClass)) {
			keyType = StorageTypeEnum.BYTES;
		}
		
		String colFamily = def.getName();
		Info info = createInfo(colFamily, type, keyType);
		String lowerCaseName = colFamily.toLowerCase();
		existingColumnFamilies2.put(lowerCaseName, info);
	}
	
	private String formName(Class class1, Class class2) {
		return CompositeType.class.getName()+"("+class1.getName()+","+class2.getName()+")";
	}

	@SuppressWarnings("unchecked")
	public Info createInfo(String colFamily, ColumnType type, StorageTypeEnum keyType) {
		if(keyType == null)
			return null;
		
		Info info = new Info();
		info.setColumnType(type);
		info.setRowKeyType(keyType);
		
		//should we cache this and just look it up each time or is KISS fine for now....
		ColumnFamily cf = null;
		switch(type) {
			case ANY_EXCEPT_COMPOSITE:
				cf = new ColumnFamily(colFamily, BytesArraySerializer.get(), BytesArraySerializer.get());
				break;
			case COMPOSITE_DECIMALPREFIX:
			case COMPOSITE_INTEGERPREFIX:
			case COMPOSITE_STRINGPREFIX:
				com.netflix.astyanax.serializers.
				AnnotatedCompositeSerializer<GenericComposite> eventSerializer = new AnnotatedCompositeSerializer<GenericComposite>(GenericComposite.class);
				cf = new ColumnFamily<byte[], GenericComposite>(colFamily, BytesArraySerializer.get(), eventSerializer);
				info.setCompositeSerializer(eventSerializer);
				break;
			default:
				throw new UnsupportedOperationException("type not supported yet="+type);
		}
		
		info.setColumnFamilyObj(cf);
		return info;
	}
	
	public Info lookupOrCreate2(String colFamily, NoSqlEntityManager mgr) {
		//There is a few possibilities here
		//1. Another server already created the CF while we were online in which case we just need to load it into memory
		//2. No one has created the CF yet
		
		//fetch will load from cassandra if we don't have it in-memory
		Info info = fetchColumnFamilyInfo(colFamily);
		if(info == null) {
			//no one has created the CF yet so we need to create it.
			createColFamily(colFamily, mgr);
		}
		
		return fetchColumnFamilyInfo(colFamily);
	}

	public Info fetchColumnFamilyInfo(String colFamily) {
		String cf = colFamily.toLowerCase();
		Info info = existingColumnFamilies2.get(cf);
		//in rare circumstances, there may be a new column family that was created by another server we need to load into
		//memory for ourselves
		if(info == null) {
			info = tryToLoadColumnFamily(colFamily);
		}
		
		return info;
	}

	private Info tryToLoadColumnFamily(String colFamily) {
		try {
			return tryToLoadColumnFamilyImpl(colFamily);
		} catch(ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	private Info tryToLoadColumnFamilyImpl(String colFamily) throws ConnectionException {
		synchronized(colFamily.intern()) {
			log.info("Column family NOT found in-memory="+colFamily+", CHECK and LOAD from Cassandra if available");
			String cf = colFamily.toLowerCase();
			Info info = existingColumnFamilies2.get(cf);
			if(info != null) {//someone else beat us into the synchronization block
				log.info("NEVER MIND, someone beat us to loading it into memory, it is now there="+cf);
				return info;
			}

			//perhaps the schema is changing and was caused by someone else, let's wait until it stablizes
			waitForNodesToBeUpToDate(null, 30000);
			
			//NOW, the schema appears stable, let's get that column family and load it
			KeyspaceDefinition keySpaceMeta = keyspace.describeKeyspace();
			ColumnFamilyDefinition def = keySpaceMeta.getColumnFamily(colFamily);
			if(def == null) {
				log.info("Well, we did NOT find any column family="+colFamily+" to load in cassandra");
				return null;
			} 
			log.info("coooool, we found a new column family="+colFamily+" to load so we are going to load that for you so every future operation is FAST");
			loadColumnFamily(def);
			return existingColumnFamilies2.get(cf);
		}
	}

	private synchronized void createColFamily(String colFamily, NoSqlEntityManager mgr) {
		try {
			createColFamilyImpl(colFamily, mgr);
		} catch(Exception e) {
			log.warn("Exception creating col family but may because another node just did that at the same time!!! so this is normal if it happens very rarely", e);
			//try to continue now...
		}
	}
	
	private synchronized void createColFamilyImpl(String colFamily, NoSqlEntityManager mgr) {
		if(existingColumnFamilies2.get(colFamily.toLowerCase()) != null)
			return;
			
		log.info("CREATING column family="+colFamily+" in cassandra");
		
		DboTableMeta cf = dbMetaFromOrmOnly.getMeta(colFamily);
		if(cf == null) {
			//check the database now for the meta since it was not found in the ORM meta data.  This is for
			//those that are modifying meta data themselves
			cf = mgr.find(DboTableMeta.class, colFamily);
			log.info("cf from db="+cf);
		}
		
		
		if(cf == null) {
			throw new IllegalStateException("Column family='"+colFamily+"' was not found AND we looked up meta data for this column" +
					" family to create it AND we could not find that data so we can't create it for you");
		}

		ColumnFamilyDefinition def = cluster.makeColumnFamilyDefinition()
			    .setName(colFamily)
			    .setKeyspace(keyspace.getKeyspaceName());

		StorageTypeEnum rowKeyType = cf.getIdColumnMeta().getStorageType();
		StorageTypeEnum type = cf.getColNamePrefixType();
		def = addRowKeyValidation(cf, def);
		def = setColumnNameCompareType(cf, type, def);
		
		ColumnType colType = ColumnType.ANY_EXCEPT_COMPOSITE;
		if(type == StorageTypeEnum.STRING)
			colType = ColumnType.COMPOSITE_STRINGPREFIX;
		else if(type == StorageTypeEnum.INTEGER)
			colType = ColumnType.COMPOSITE_INTEGERPREFIX;
		else if(type == StorageTypeEnum.DECIMAL)
			colType = ColumnType.COMPOSITE_DECIMALPREFIX;
		
		addColumnFamily(def);
		Info info = createInfo(colFamily, colType, rowKeyType);
		String cfName = colFamily.toLowerCase();
		existingColumnFamilies2.put(cfName, info);
	}
	
	private ColumnFamilyDefinition setColumnNameCompareType(DboTableMeta cf,
			StorageTypeEnum type, ColumnFamilyDefinition def2) {
		ColumnFamilyDefinition def = def2;
		if(type == null) {
			StorageTypeEnum t = cf.getNameStorageType();
			if(t == StorageTypeEnum.STRING)
				def = def.setComparatorType("UTF8Type");
			else if(t == StorageTypeEnum.DECIMAL)
				def = def.setComparatorType("DecimalType");
			else if(t == StorageTypeEnum.INTEGER)
				def = def.setComparatorType("IntegerType");
			else
				throw new UnsupportedOperationException("type="+type+" is not supported at this time");
		} else if(type == StorageTypeEnum.STRING) {
			def = def.setComparatorType("CompositeType(UTF8Type, BytesType)");
		} else if(type == StorageTypeEnum.INTEGER) {
			def = def.setComparatorType("CompositeType(IntegerType, BytesType)");
		} else if(type == StorageTypeEnum.DECIMAL) {
			def = def.setComparatorType("CompositeType(DecimalType, BytesType)");
		}
		else
			throw new UnsupportedOperationException("Not supported yet, we need a BigDecimal comparator type here for sure");
		return def;
	}

	private ColumnFamilyDefinition addRowKeyValidation(DboTableMeta cf,
			ColumnFamilyDefinition def2) {
		ColumnFamilyDefinition def = def2;
		DboColumnMeta idColumnMeta = cf.getIdColumnMeta();
		StorageTypeEnum rowKeyType = idColumnMeta.getStorageType();
		switch (rowKeyType) {
		case STRING:
			def = def.setKeyValidationClass("UTF8Type");
			break;
		case INTEGER:
			def = def.setKeyValidationClass("DecimalType");
			break;
		case DECIMAL:
			def = def.setKeyValidationClass("IntegerType");
			break;
		case BYTES:
			break;
		default:
			throw new UnsupportedOperationException("type="+rowKeyType+" is not supported at this time");
		}
		return def;
	}

	private void addColumnFamily(ColumnFamilyDefinition def) {
		try {
			String id = cluster.addColumnFamily(def);
			long timeout = 30000;
			waitForNodesToBeUpToDate(id, timeout);
		} catch (ConnectionException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	public void waitForNodesToBeUpToDate(String id, long timeout)
			throws ConnectionException {
		if(id != null)
			log.info("LOOP until all nodes have same schema version id="+id+" OR timeout in "+timeout+" milliseconds");
		else
			log.info("LOOP until all nodes have same schema version OR timeout in "+timeout+" milliseconds");
		
		long currentTime = System.currentTimeMillis();
		while(true) {
			Map<String, List<String>> describeSchemaVersions = cluster.describeSchemaVersions();
			long now = System.currentTimeMillis();
			if(describeSchemaVersions.size() == 1) {
				String key = describeSchemaVersions.keySet().iterator().next();
				if(id != null && !id.equals(key)) {
					log.warn("BUG, in cassandra? id we upgraded schema to="+id+" but the schema on all nodes is now="+key);
				}
				assert id == null || key.equals(id) : "The key and id should be equal!!!! as it is updating to our schema";
				break;
			} else if(now >= currentTime+timeout) {
				log.warn("All nodes are still not up to date, but we have already waited 30 seconds!!! so we are returning");
				break;
			}
			
			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
