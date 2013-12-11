package com.alvazan.test;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;

public class FactorySingleton {
	
	private static final Logger log = LoggerFactory.getLogger(FactorySingleton.class);
	private static NoSqlEntityManagerFactory factory;
	
	private static DbTypeEnum serverType;

	public static Config getConfigForAllTests() {
		/**************************************************
		 * FLIP THIS BIT TO CHANGE FROM CASSANDRA TO ANOTHER ONE
		 **************************************************/
		String clusterName = "PlayCluster";
		//serverType = DbTypeEnum.CASSANDRA;
		//serverType = DbTypeEnum.MONGODB;
		serverType = DbTypeEnum.IN_MEMORY;
		//serverType = DbTypeEnum.HBASE;
		//serverType = DbTypeEnum.CQL;
		String seeds = "localhost:9160";
		if (serverType.equals(DbTypeEnum.MONGODB))
			seeds = "localhost:27017";
		if (serverType.equals(DbTypeEnum.HBASE))
			seeds = "localhost";
		return new Config(serverType, clusterName, seeds);
	}
	
	public synchronized static NoSqlEntityManagerFactory createFactoryOnce() {
		if(factory == null) {
			Config config = getConfigForAllTests();
			//We used this below commented out seeds to test our suite on a cluster of 6 nodes to see if any issues pop up with more
			//nodes using the default astyanax consistency levels which I believe for writes and reads are both QOURUM
			//which is perfect for us as we know we will get the latest results
			//String seeds = "a1.bigde.nrel.gov:9160,a2.bigde.nrel.gov:9160,a3.bigde.nrel.gov:9160";
			Map<String, Object> props = new HashMap<String, Object>();
			factory = createFactory(config, props);
		}
		return factory;
	}

	public static NoSqlEntityManagerFactory createFactory(Config config, Map<String, Object> props) {
		log.info("CREATING FACTORY FOR TESTS");
		props.put(Bootstrap.AUTO_CREATE_KEY, "create");
		switch (config.getServerType()) {
		case IN_MEMORY:
			//nothing to do
			break;
		case CASSANDRA:
			Bootstrap.createAndAddBestCassandraConfiguration(props, config.getClusterName(), "PlayOrmKeyspace", config.getSeeds());
			break;
		case MONGODB:
			Bootstrap.createAndAddBestMongoDbConfiguration(props, config.getClusterName(), "PlayOrmKeyspace", config.getSeeds());
			break;
		case HBASE:
			Bootstrap.createAndAddBestHBaseConfiguration(props, config.getClusterName(), "PlayOrmKeyspace", config.getSeeds());
			break;
	     case CQL:
	            Bootstrap.createAndAddBestCqlConfiguration(props, config.getClusterName(), "PlayOrmKeyspace", config.getSeeds());
	            break;
		default:
			throw new UnsupportedOperationException("not supported yet, server type="+config.getServerType());
		}

		NoSqlEntityManagerFactory factory = Bootstrap.create(config.getServerType(), props, null, null);
		return factory;
	}

    public static DbTypeEnum getServerType() {
        return serverType;
    }
	
}
