package com.alvazan.test;

import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Activity;

public class TestIndexTypes {

	private static Index<Activity> index;

	private static NoSqlEntityManagerFactory factory;
	private NoSqlEntityManager mgr;

	@BeforeClass
	public static void setup() {
		factory = FactorySingleton.createFactoryOnce();
	}
	
	@Before
	public void createEntityManager() {
		mgr = factory.createEntityManager();
		index = setupRecords();
	}
	@After
	public void clearDatabase() {
		NoSqlEntityManager other = factory.createEntityManager();
		other.clearDbAndIndexesIfInMemoryType();
	}
	
	@Test
	public void testBasicString() {
		List<Activity> findByName = Activity.findByName(index, "hello");
		Assert.assertEquals(1, findByName.size());
		
		List<Activity> zero = Activity.findByName(index, "asdf");
		Assert.assertEquals(0, zero.size());
	}

	@Test
	public void testBasicBoolean() {
		List<Activity> list = Activity.findByCool(index, true);
		Assert.assertEquals(1, list.size());
		
		List<Activity> zero = Activity.findByCool(index, false);
		Assert.assertEquals(0, zero.size());
	}
	
	@Test
	public void testBasicLong() {
		List<Activity> list = Activity.findNumTimes(index, 5L);
		Assert.assertEquals(1, list.size());
		
		List<Activity> zero = Activity.findNumTimes(index, 0L);
		Assert.assertEquals(0, zero.size());		
	}

	@Test
	public void testBasicFloat() {
		List<Activity> list = Activity.findByFloat(index, 5.65f);
		Assert.assertEquals(1, list.size());
		
		List<Activity> zero = Activity.findByFloat(index, 5.66f);
		Assert.assertEquals(0, zero.size());		
	}
	
	private Index<Activity> setupRecords() {
		Activity act = new Activity();
		act.setName("hello");
		act.setMyFloat(5.65f);
		act.setUniqueColumn("notunique");
		act.setNumTimes(5);
		act.setIsCool(true);
		mgr.put(act);
		
		Index<Activity> index = mgr.getIndex(Activity.class, "/activity/byaccount/account1");
		index.addToIndex(act);
		
		mgr.flush();
		return index;
	}

}
