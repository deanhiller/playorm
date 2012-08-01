package com.alvazan.test;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.User;

public class TestIndexesNew {

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
		other.clearDbAndIndexesIfInMemoryType();
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
		
		User newResult = User.findByName(mgr, user.getName());
		Assert.assertNull(newResult);
		
		User newerResult = User.findByName(mgr, result.getName());
		Assert.assertEquals(result.getName(), newerResult.getName());
		
	}
	
	//@Test
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
	
}
