package com.alvazan.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Activity;
import com.alvazan.test.db.PartitionedSingleTrade;
import com.alvazan.test.db.TimeSeriesData;

public class TestVirtualCf {

	private static NoSqlEntityManagerFactory factory;
	private NoSqlEntityManager mgr;

	@BeforeClass
	public static void setup() {
		factory = FactorySingleton.createFactoryOnce();
	}
	
	@Before
	public void createEntityManager() {
		mgr = factory.createEntityManager();
	}
	@After
	public void clearDatabase() {
		NoSqlEntityManager other = factory.createEntityManager();
		other.clearDatabase(true);
	}
	
	@Test
	public void testWriteRead() {
		//let's use TWO entities that share the same columnFamily AND use the same key in both to make sure
		//the prefix stuff works just fine...
		
		Activity act1 = new Activity("myid");
		act1.setName("myname");
		mgr.put(act1);
		
		PartitionedSingleTrade trade = new PartitionedSingleTrade();
		trade.setId("myid");
		trade.setNumber(89);
		mgr.put(trade);
		
		//throw an a guy with Long key types as well...
		TimeSeriesData d = new TimeSeriesData();
		d.setKey(897L);
		d.setSomeName("qwer");
		mgr.put(d);
		mgr.flush();
		
		//unfortunately, the two rows are written as one (ie. MERGED) so
		//to really TEST this out, we remove the trade row to make sure we still have the Activity
		//row
		mgr.remove(trade);
		mgr.flush();
		
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		Activity act = mgr2.find(Activity.class, act1.getId());
		Assert.assertEquals(act1.getName(), act.getName());
		
		PartitionedSingleTrade r = mgr2.find(PartitionedSingleTrade.class, trade.getId());
		Assert.assertNull(r);
		
		TimeSeriesData d2 = mgr2.find(TimeSeriesData.class, d.getKey());
		Assert.assertEquals(d.getSomeName(), d2.getSomeName());
	}
	
}
