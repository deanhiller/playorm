package com.alvazan.test;

import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Activity;

public class TestIndexTypes {

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
		List<Activity> findByName = Activity.findByName(mgr, "hello");
		Assert.assertEquals(1, findByName.size());
		
		List<Activity> zero = Activity.findByName(mgr, "asdf");
		Assert.assertEquals(0, zero.size());
	}

	@Test
	public void testBasicBoolean() {
		List<Activity> list = Activity.findByCool(mgr, true);
		Assert.assertEquals(1, list.size());
		
		List<Activity> zero = Activity.findByCool(mgr, false);
		Assert.assertEquals(0, zero.size());
	}
	
	@Test
	public void testBasicLong() {
		List<Activity> list = Activity.findNumTimes(mgr, 5L);
		Assert.assertEquals(1, list.size());
		
		List<Activity> zero = Activity.findNumTimes(mgr, 0L);
		Assert.assertEquals(0, zero.size());
	}

	@Test
	public void testBasicFloat() {
		List<Activity> list = Activity.findByFloat(mgr, 5.65f);
		Assert.assertEquals(1, list.size());
		
		List<Activity> zero = Activity.findByFloat(mgr, 5.66f);
		Assert.assertEquals(0, zero.size());		
	}
	
	private void setupRecords() {
		Activity act = new Activity();
		act.setName("hello");
		act.setMyFloat(5.65f);
		act.setUniqueColumn("notunique");
		act.setNumTimes(5);
		act.setIsCool(true);
		mgr.put(act);
		
		//Everything is null for this activity so queries above should not find him...
		Activity act2 = new Activity();
		act2.setNumTimes(58);
		mgr.put(act2);
		
		mgr.flush();
	}

}
