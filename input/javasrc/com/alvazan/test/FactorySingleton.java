package com.alvazan.test;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;

public class FactorySingleton {

	private static final Logger log = LoggerFactory.getLogger(FactorySingleton.class);
	
	private static NoSqlEntityManagerFactory factory;
	
	public synchronized static NoSqlEntityManagerFactory createFactoryOnce() {
		if(factory == null) {
			/**************************************************
			 * FLIP THIS BIT TO CHANGE FROM CASSANDRA TO ANOTHER ONE
			 **************************************************/
			String clusterName = "PlayCluster";
			DbTypeEnum serverType = DbTypeEnum.IN_MEMORY;
			String host = "localhost";
			createFactory(serverType, clusterName, host);
		}
		return factory;
	}

	private static void createFactory(DbTypeEnum server, String clusterName, String host) {
		log.info("CREATING FACTORY FOR TESTS");
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(Bootstrap.AUTO_CREATE_KEY, "create");
		switch (server) {
		case IN_MEMORY:
			//nothing to do
			break;
		case CASSANDRA:
			Builder builder = buildBuilder(clusterName, host);
			props.put(Bootstrap.CASSANDRA_BUILDER, builder);
			break;
		default:
			throw new UnsupportedOperationException("not supported yet, server type="+server);
		}

		factory = Bootstrap.create(server, props, null, null);
	}
	
	public static Builder buildBuilder(String clusterName, String host) {
		Builder builder = new AstyanaxContext.Builder()
	    .forCluster(clusterName)
	    .forKeyspace("PlayKeyspace")
	    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
	        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
	    )
	    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
	        .setMaxConnsPerHost(2)
	        .setInitConnsPerHost(2)
	        .setSeeds(host+":9160")
	    )
	    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());		
		
		return builder;
	}
}
