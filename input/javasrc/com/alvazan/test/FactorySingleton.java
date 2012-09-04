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
import com.netflix.astyanax.model.ConsistencyLevel;

public class FactorySingleton {

	private static final Logger log = LoggerFactory.getLogger(FactorySingleton.class);
	
	private static NoSqlEntityManagerFactory factory;
	
	public synchronized static NoSqlEntityManagerFactory createFactoryOnce() {
		if(factory == null) {
			/**************************************************
			 * FLIP THIS BIT TO CHANGE FROM CASSANDRA TO ANOTHER ONE
			 **************************************************/
			String clusterName = "PlayCluster";
			DbTypeEnum serverType = DbTypeEnum.CASSANDRA;
			String seeds = "localhost:9160";
			//We used this below commented out seeds to test our suite on a cluster of 6 nodes to see if any issues pop up with more
			//nodes using the default astyanax consistency levels which I believe for writes and reads are both QOURUM
			//which is perfect for us as we know we will get the latest results
			//String seeds = "a1.bigde.nrel.gov:9160,a2.bigde.nrel.gov:9160,a3.bigde.nrel.gov:9160";
			createFactory(serverType, clusterName, seeds);
		}
		return factory;
	}

	private static void createFactory(DbTypeEnum server, String clusterName, String seeds) {
		log.info("CREATING FACTORY FOR TESTS");
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(Bootstrap.AUTO_CREATE_KEY, "create");
		switch (server) {
		case IN_MEMORY:
			//nothing to do
			break;
		case CASSANDRA:
			Builder builder = buildBuilder(clusterName, seeds);
			props.put(Bootstrap.CASSANDRA_BUILDER, builder);
			break;
		default:
			throw new UnsupportedOperationException("not supported yet, server type="+server);
		}

		factory = Bootstrap.create(server, props, null, null);
	}
	
	public static Builder buildBuilder(String clusterName, String seeds) {
		Builder builder = new AstyanaxContext.Builder()
	    .forCluster(clusterName)
	    .forKeyspace("PlayKeyspace")
	    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
	        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
	    )
	    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
	        .setMaxConnsPerHost(2)
	        .setInitConnsPerHost(2)
	        .setSeeds(seeds)
	    )
	    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());		
		
		if(!"localhost:9160".equals(seeds)) {
			//for a multi-node cluster, we want the test suite using quorum on writes and
			//reads so we have no issues...
			AstyanaxConfigurationImpl config = new AstyanaxConfigurationImpl();
			config.setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_QUORUM);
			config.setDefaultReadConsistencyLevel(ConsistencyLevel.CL_QUORUM);
			builder = builder.withAstyanaxConfiguration(config);
		}
		
		return builder;
	}
}
