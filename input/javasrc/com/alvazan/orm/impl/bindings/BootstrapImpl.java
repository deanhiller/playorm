package com.alvazan.orm.impl.bindings;

import java.util.Map;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.layer0.base.BaseEntityManagerFactoryImpl;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;

public class BootstrapImpl extends Bootstrap {

	@SuppressWarnings("rawtypes")
	@Override
	public NoSqlEntityManagerFactory createInstance(DbTypeEnum type, Map<String, Object> properties, Map<Class, Converter> converters, ClassLoader cl2) {
		Object spiImpl = properties.get(Bootstrap.SPI_IMPL);
		NoSqlRawSession temp = null;
		if(spiImpl != null && spiImpl instanceof NoSqlRawSession) {
			temp = (NoSqlRawSession) spiImpl;
		}
		
		Injector injector = Guice.createInjector(new ProductionBindings(type, temp));
		NoSqlEntityManagerFactory factory = injector.getInstance(NoSqlEntityManagerFactory.class);

		Named named = Names.named("logger");
		Key<NoSqlRawSession> key = Key.get(NoSqlRawSession.class, named);
		NoSqlRawSession inst = injector.getInstance(key);
		inst.start(properties);
		
		//why not just add setInjector() and setup() in NoSqlEntityManagerFactory
		BaseEntityManagerFactoryImpl impl = (BaseEntityManagerFactoryImpl)factory;
		impl.setInjector(injector);
		
		ClassLoader cl = cl2;
		if(cl == null)
			cl = BootstrapImpl.class.getClassLoader();
		//The expensive scan all entities occurs here...
		impl.setup(properties, converters, cl);
		
		return impl;
	}
	@Override
	protected void createBestCassandraConfig(Map<String, Object> properties,
			String clusterName, String keyspace2, String seeds2) {
		Builder builder = new AstyanaxContext.Builder()
	    .forCluster(clusterName)
	    .forKeyspace(keyspace2)
	    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
	        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
	    )
	    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
	        .setMaxConnsPerHost(2)
	        .setInitConnsPerHost(2)
	        .setSeeds(seeds2)
	    )
	    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());
		
		
		if(!"localhost:9160".equals(seeds2)) {
			if(!seeds2.contains(","))
				throw new IllegalArgumentException("You must specify a comma delimited list of seeds OR 'localhost:9160' as the seed");
			//for a multi-node cluster, we want the test suite using quorum on writes and
			//reads so we have no issues...
			AstyanaxConfigurationImpl config = new AstyanaxConfigurationImpl();
			config.setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_QUORUM);
			config.setDefaultReadConsistencyLevel(ConsistencyLevel.CL_QUORUM);
			builder = builder.withAstyanaxConfiguration(config);
		}		
		properties.put(Bootstrap.CASSANDRA_BUILDER, builder);
	}


}
