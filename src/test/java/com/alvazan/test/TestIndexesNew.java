package com.alvazan.test;

import org.junit.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.TimeSeriesData;
import com.alvazan.test.db.User;

public class TestIndexesNew {

	private static final Logger log = LoggerFactory.getLogger(TestIndexesNew.class);
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
		try {
			other.clearDatabase(true);
		} catch(Exception e) {
			log.warn("Could not clean up properly", e);
		}
	}
	
	@Test
	public void testBasicChangeToIndex() {
		User user = new User();
		user.setName("dean");
		user.setLastName("hiller");
		
		mgr.put(user);
		
		mgr.flush();
		
		User result = User.findByName(mgr, user.getName());
		Assert.assertEquals(user.getName(), result.getName());
		
		result.setName("dave");
		mgr.put(result);
		mgr.flush();
		
		User newResult = User.findByName(mgr, user.getName());
		Assert.assertNull(newResult);
		
		User newerResult = User.findByName(mgr, result.getName());
		Assert.assertEquals(result.getName(), newerResult.getName());
		
	}
	
	@Test
	public void testBasicRemove() {
		User user = new User();
		user.setName("dean");
		user.setLastName("hiller");
		
		mgr.put(user);
		
		mgr.flush();
		
		User result = User.findByName(mgr, user.getName());
		Assert.assertEquals(user.getName(), result.getName());
		
		mgr.remove(user);
		
		mgr.flush();
		
		User newResult = User.findByName(mgr, user.getName());
		Assert.assertNull(newResult);
	}
	
	@Test
	public void testPrimaryKeyLongIndex() {
		TimeSeriesData data = new TimeSeriesData();
		data.setKey(67L);
		data.setSomeName("dean");
		
		mgr.put(data);
		mgr.flush();

		TimeSeriesData newData = TimeSeriesData.findById(mgr, data.getKey());
		Assert.assertEquals(data.getSomeName(), newData.getSomeName());
	}
	
	@Test
	public void testFloatKey() {
		TimeSeriesData data = new TimeSeriesData();
		data.setKey(67L);
		data.setSomeName("dean");
		data.setTemp(67.8f);
		
		mgr.put(data);
		mgr.flush();

		TimeSeriesData newData = TimeSeriesData.findByTemp(mgr, 67.8f);
		Assert.assertEquals(data.getSomeName(), newData.getSomeName());
	}
}
