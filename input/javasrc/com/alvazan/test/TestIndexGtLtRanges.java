package com.alvazan.test;

import java.util.List;

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

public class TestIndexGtLtRanges {

	private static final Logger log = LoggerFactory.getLogger(TestIndexGtLtRanges.class);
	
	private static NoSqlEntityManagerFactory factory;
	private NoSqlEntityManager mgr;

	@BeforeClass
	public static void setup() {
		factory = FactorySingleton.createFactoryOnce();
	}
	
	@Before
	public void createEntityManager() {
		mgr = factory.createEntityManager();
		setupRecords();
	}
	@After
	public void clearDatabase() {
		NoSqlEntityManager other = factory.createEntityManager();
		other.clearDatabase(true);
	}
	
	@Test
	public void testBasicString() throws InterruptedException {
		
		List<Activity> all = Activity.findAll(mgr, 2);
		Assert.assertEquals(5, all.size());
		
		//should retrieve from the cache...
		Activity act = mgr.find(Activity.class, all.get(0).getId());
		
		List<Activity> list = Activity.findBetween(mgr, 4, 7);
		Assert.assertEquals(2, list.size());
		if(list.get(0).getNumTimes() != 4) {
			Activity item1 = list.get(0);
			Activity item2 = list.get(1);
			log.info("shutdown now.  failure item1="+item1.getNumTimes()+"/"+item1.getName()+"  item2="+item2.getNumTimes()+"/"+item2.getName());
			Thread.sleep(600000);
		}
		
		Assert.assertEquals(4, list.get(0).getNumTimes());
		Assert.assertEquals(5, list.get(1).getNumTimes());
		
		List<Activity> list2 = Activity.findBetween2(mgr, 4, 7);
		Assert.assertEquals(2, list2.size());
		if(list2.get(0).getNumTimes() != 5) {
			Activity item1 = list2.get(0);
			Activity item2 = list2.get(1);
			log.info("shutdown now.  failure item1="+item1.getNumTimes()+"/"+item1.getName()+"  item2="+item2.getNumTimes()+"/"+item2.getName());
			Thread.sleep(600000);
		}
		Assert.assertEquals(5, list2.get(0).getNumTimes());
		Assert.assertEquals(7, list2.get(1).getNumTimes());
	
		List<Activity> list3 = Activity.findAbove(mgr, 7);
		Assert.assertEquals(1, list3.size());
		
		List<Activity> list4 = Activity.findBelow(mgr, 4);
		Assert.assertEquals(1, list4.size());
		
	}

	private void setupRecords() {
		Activity act1 = new Activity();
		act1.setNumTimes(3);
		mgr.put(act1);
		
		Activity act2 = new Activity();
		act2.setName("aaaaaa");
		act2.setNumTimes(4);
		mgr.put(act2);

		Activity act3 = new Activity();
		act3.setName("bbbbbbb");
		act3.setNumTimes(5);
		mgr.put(act3);
		
		Activity act4 = new Activity();
		act4.setNumTimes(7);
		mgr.put(act4);
		
		Activity act5 = new Activity();
		act5.setNumTimes(8);
		mgr.put(act5);
		
		mgr.flush();
	}

}
