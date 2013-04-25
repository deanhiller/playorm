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

import com.alvazan.orm.api.z8spi.ColumnType;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.SpiConstants;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@SuppressWarnings("rawtypes")
public class ColumnFamilyHelper {
	private static final Logger log = LoggerFactory.getLogger(ColumnFamilyHelper.class);
	
	@Inject
	private DboDatabaseMeta dbMetaFromOrmOnly;
	
	private Map<String, Info> cfNameToCassandra = new HashMap<String, Info>();
	private Map<String, String> virtualToCfName = new HashMap<String, String>();
	
	private Keyspace keyspace;
	private Cluster cluster;
	private String keyspaceName;

	private AstyanaxContext<Cluster> clusterContext;

	private AstyanaxContext<Keyspace> context;
	
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
		Object builderObj = properties.get(SpiConstants.CASSANDRA_BUILDER);
//		if(seedsObj == null || !(seedsObj instanceof String))
//			throw new IllegalArgumentException("The property Bootstrap.HOST was not in the Map or was in the Map but not as a String");
//		else if(keyspaceNameObj == null || !(keyspaceNameObj instanceof String))
//			throw new IllegalArgumentException("The property Bootstrap.KEYSPACE was not in the Map or was in the Map but not as a String");
//		else if(clusterNameObj == null || !(clusterNameObj instanceof String))
//			throw new IllegalArgumentException("The property Bootstrap.CLUSTER_NAME was not in the Map or was in the Map but not as a String");
		if(builderObj == null || !(builderObj instanceof Builder))
			throw new IllegalArgumentException("The property Bootstrap.CASSANDRA_BUILDER was not in the Map or was in Map but was not of type Builder and must be supplied when using Cassandra plugin");

		Builder builder = (Builder) builderObj;
		clusterContext = builder.buildCluster(ThriftFamilyFactory.getInstance());
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
		
		context = builder.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		
		keyspace = context.getEntity();

		KeyspaceDefinition keySpaceMeta = keyspace.describeKeyspace();
		
		findExistingColumnFamilies(keySpaceMeta);
		log.info("On keyspace="+keyspace.getKeyspaceName()+"Existing column families="+cfNameToCassandra.keySet()+"\nNOTE: WE WILL CREATE " +
				"new column families automatically as you save entites that have no column family");
	}

	private void findExistingColumnFamilies(KeyspaceDefinition keySpaceMeta) {
		List<ColumnFamilyDefinition> cfList = keySpaceMeta.getColumnFamilyList();
		for(ColumnFamilyDefinition def : cfList) {
			loadColumnFamilyImpl(def);
		}
	}

	private void loadColumnFamily(ColumnFamilyDefinition def, String virtCf,
			String realCf) {
		loadColumnFamilyImpl(def);
		virtualToCfName.put(virtCf, realCf);
	}
	
	private void loadColumnFamilyImpl(ColumnFamilyDefinition def) {
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
		} else 
			keyType = StorageTypeEnum.BYTES;
		
		String colFamily = def.getName();
		Info info = createInfo(colFamily, type, keyType);
		String lowerCaseName = colFamily.toLowerCase();
		cfNameToCassandra.put(lowerCaseName, info);
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
	
	public Info lookupOrCreate2(String virtualCf, MetaLookup ormSession) {
		//There is a few possibilities here
		//1. Another server already created the CF while we were online in which case we just need to load it into memory
		//2. No one has created the CF yet
		
		//fetch will load from cassandra if we don't have it in-memory
		Info origInfo = fetchColumnFamilyInfo(virtualCf, ormSession);
		Exception ee = null;
		if(origInfo == null) {
			//no one has created the CF yet so we need to create it.
			ee = createColFamily(virtualCf, ormSession);
		}
		
		//Now check...maybe someone else created it...or we did successfully....
		Info info = fetchColumnFamilyInfo(virtualCf, ormSession);
		if(info == null)
			throw new RuntimeException("Could not create and could not find virtual or real colfamily="+virtualCf+" see chained exception AND it could be your name is not allowed as a valid cassandra Column Family name", ee);
		return info;
	}

	public Info fetchColumnFamilyInfo(String virtualCf, MetaLookup lookup) {
		Info info = lookupVirtCf(virtualCf);
		if(info != null)
			return info;
		
		//in rare circumstances, there may be a new column family that was created by another server we need to load into
		//memory for ourselves
		return tryToLoadColumnFamilyVirt(virtualCf, lookup);
	}

	private Info lookupVirtCf(String virtualCf) {
		String cfName = virtualToCfName.get(virtualCf);
		if(log.isDebugEnabled())
			log.debug("looking up virtualcf="+virtualCf+" actual name="+cfName);
		if(cfName == null)
			return null;
		
		Info info = cfNameToCassandra.get(cfName);
		if(log.isDebugEnabled())
			log.debug("virtual="+virtualCf+" actual name="+cfName+" cf info="+info);
		return info;
	}

	private Info tryToLoadColumnFamilyVirt(String virtColFamily, MetaLookup lookup) {
		try {
			long start = System.currentTimeMillis();
			Info info = tryToLoadColumnFamilyImpl(virtColFamily, lookup);
			long total = System.currentTimeMillis() - start;
			if(log.isInfoEnabled())
				log.info("Total time to LOAD column family meta from cassandra="+total);
			return info;
		} catch(ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	private Info tryToLoadColumnFamilyImpl(String virtCf, MetaLookup lookup) throws ConnectionException {
		synchronized(virtCf.intern()) {
			log.info("Column family NOT found in-memory="+virtCf+", CHECK and LOAD from Cassandra if available");
			
			String cfName = virtualToCfName.get(virtCf);
			if(cfName != null) {
				Info info = cfNameToCassandra.get(cfName);
				if(info != null) {
					log.info("NEVER MIND, someone beat us to loading it into memory, it is now there="+virtCf+"(realcf="+cfName+")");
					return cfNameToCassandra.get(cfName);
				}
			}

			DboTableMeta table = loadFromInMemoryOrDb(virtCf, lookup);
			
			String realCf = table.getRealColumnFamily();

			String realCfLower = realCf.toLowerCase();
			Info info = cfNameToCassandra.get(realCfLower);
			if(info != null) {
				log.info("Virt CF="+virtCf+" already exists and real colfamily="+realCf+" already exists so return it");
				//Looks like it already existed
				String cfLowercase = realCf.toLowerCase();
				virtualToCfName.put(virtCf, cfLowercase);
				return info;
			}
			
			//perhaps the schema is changing and was caused by someone else, let's wait until it stablizes
			waitForNodesToBeUpToDate(null, 300000);
			
			//NOW, the schema appears stable, let's get that column family and load it
			KeyspaceDefinition keySpaceMeta = keyspace.describeKeyspace();
			ColumnFamilyDefinition def = keySpaceMeta.getColumnFamily(realCf);
			if(def == null) {
				log.info("Well, we did NOT find any column family="+realCf+" to load in cassandra(from virt="+virtCf+")");
				return null;
			} 
			log.info("coooool, we found a new column family="+realCf+"(virt="+virtCf+") to load so we are going to load that for you so every future operation is FAST");
			loadColumnFamily(def, virtCf, realCf);
			
			return lookupVirtCf(virtCf);
		}
	}

	private DboTableMeta loadFromInMemoryOrDb(String virtCf, MetaLookup lookup) {
		log.info("looking up meta="+virtCf+" so we can add table to memory(one time operation)");
		DboTableMeta meta = dbMetaFromOrmOnly.getMeta(virtCf);
		if(meta != null) {
			log.info("found meta="+virtCf+" locally");
			return meta;
		}
		
		DboTableMeta table = lookup.find(DboTableMeta.class, virtCf);
		if(table == null)
			throw new IllegalArgumentException("We can't load the meta for virtual or real CF="+virtCf+" because there is not meta found in DboTableMeta table");
		log.info("found meta="+virtCf+" in database");
		return table;
	}

	private synchronized Exception createColFamily(String virtualCf, MetaLookup ormSession) {
		try {
			long start = System.currentTimeMillis();
			createColFamilyImpl(virtualCf, ormSession);
			long total = System.currentTimeMillis() - start;
			if(log.isInfoEnabled())
				log.info("Total time to CREATE column family in cassandra and wait for all nodes to update="+total);
		} catch(Exception e) {
			if(log.isTraceEnabled())
				log.trace("maybe someone else created at same time, so hold off on throwing exception", e);
			return e;
		}
		return null;
	}
	
	private synchronized void createColFamilyImpl(String virtualCf, MetaLookup ormSession) {
		if(lookupVirtCf(virtualCf) != null)
			return;

		String keysp = keyspace.getKeyspaceName();
		log.info("CREATING column family="+virtualCf+" in cassandra keyspace="+keysp);
		
		DboTableMeta meta = loadFromInMemoryOrDb(virtualCf, ormSession);
		log.info("CREATING REAL cf="+meta.getRealColumnFamily()+" (virtual CF="+meta.getRealVirtual()+")");

		createColFamilyInCassandra(meta);
	}

	private void createColFamilyInCassandra(DboTableMeta meta) {
		String keysp = keyspace.getKeyspaceName();
		String cfName = meta.getRealColumnFamily().toLowerCase();
		String colFamily = meta.getRealColumnFamily();
		log.info("CREATING colfamily="+cfName+" in keyspace="+keysp);
		ColumnFamilyDefinition def = cluster.makeColumnFamilyDefinition()
			    .setName(colFamily)
			    .setKeyspace(keysp);

		log.info("keyspace="+def.getKeyspace()+" col family="+def.getName());
		StorageTypeEnum rowKeyType = meta.getIdColumnMeta().getStorageType();
		ColumnType colType = ColumnType.ANY_EXCEPT_COMPOSITE;
		if(meta.isVirtualCf()) {
			rowKeyType = StorageTypeEnum.BYTES;
		} else {
			StorageTypeEnum type = meta.getColNamePrefixType();
			def = addRowKeyValidation(meta, def);
			def = setColumnNameCompareType(meta, type, def);
			
			if(type == StorageTypeEnum.STRING)
				colType = ColumnType.COMPOSITE_STRINGPREFIX;
			else if(type == StorageTypeEnum.INTEGER)
				colType = ColumnType.COMPOSITE_INTEGERPREFIX;
			else if(type == StorageTypeEnum.DECIMAL)
				colType = ColumnType.COMPOSITE_DECIMALPREFIX;
		}
		
		addColumnFamily(def);
		String virtual = meta.getColumnFamily();
		String realCf = meta.getRealColumnFamily();
		String realCfLower = realCf.toLowerCase();
		Info info = createInfo(realCf, colType, rowKeyType);
		virtualToCfName.put(virtual, realCfLower);
		cfNameToCassandra.put(realCfLower, info);
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
			def = def.setKeyValidationClass("IntegerType");
			break;
		case DECIMAL:
			def = def.setKeyValidationClass("DecimalType");
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
			OperationResult<SchemaChangeResult> result = cluster.addColumnFamily(def);
			long timeout = 300000;
			waitForNodesToBeUpToDate(result, timeout);
		} catch (ConnectionException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	public void waitForNodesToBeUpToDate(OperationResult<SchemaChangeResult> result, long timeout)
			throws ConnectionException {
		if(result != null && result.getResult() != null)
			log.info("LOOP until all nodes have same schema version id="+result.getResult().getSchemaId()+" OR timeout in "+timeout+" milliseconds");
		else
			log.info("LOOP until all nodes have same schema version OR timeout in "+timeout+" milliseconds");
		
		long currentTime = System.currentTimeMillis();
		while(true) {
			Map<String, List<String>> describeSchemaVersions = cluster.describeSchemaVersions();
			long now = System.currentTimeMillis();
			if(describeSchemaVersions.size() == 1) {
				String key = describeSchemaVersions.keySet().iterator().next();
				if(result !=null && result.getResult()!= null && !key.equals(result.getResult().getSchemaId())) {
					log.warn("BUG, in cassandra? id we upgraded schema to="+result.getResult().getSchemaId()+" but the schema on all nodes is now="+key);
				}
				if (result !=null && result.getResult()!= null)
					assert result.getResult().getSchemaId() == null || key.equals(result.getResult().getSchemaId()) : "The key and id should be equal!!!! as it is updating to our schema";
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

	public void close() {
		try {
			clusterContext.shutdown();
		} catch(Exception e) {
			log.warn("Could not shutdown properly", e);
		}
		try {
			context.shutdown();
		} catch(Exception e) {
			log.warn("Could not shutdown properly", e);
		}
	}
}
