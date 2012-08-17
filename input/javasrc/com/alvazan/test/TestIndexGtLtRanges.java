package com.alvazan.test;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Activity;

public class TestIndexGtLtRanges {

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
	public void testBasicString() {
		
		List<Activity> list = Activity.findBetween(mgr, 4, 7);
		Assert.assertEquals(2, list.size());
		Assert.assertEquals(4, list.get(0).getNumTimes());
		Assert.assertEquals(5, list.get(1).getNumTimes());
		
		List<Activity> list2 = Activity.findBetween2(mgr, 4, 7);
		Assert.assertEquals(2, list2.size());
		Assert.assertEquals(5, list2.get(0).getNumTimes());
		Assert.assertEquals(7, list2.get(1).getNumTimes());
	
		
	}

	private void setupRecords() {
		Activity act1 = new Activity();
		act1.setNumTimes(3);
		mgr.put(act1);
		
		Activity act2 = new Activity();
		act2.setNumTimes(4);
		mgr.put(act2);

		Activity act3 = new Activity();
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
