package com.alvazan.test;

import org.joda.time.LocalDateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.EntityWithDateTimeKey;
import com.alvazan.test.db.EntityWithIntKey;

public class TestKeysAndBasic {

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
	public void testIntegerKey() {
		//Activity has null reference to account
		EntityWithIntKey act = new EntityWithIntKey();
		act.setId(5);
		act.setSomething("asdf");
		
		mgr.put(act);
		mgr.flush();
		
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		EntityWithIntKey entity = mgr2.find(EntityWithIntKey.class, act.getId());
		Assert.assertEquals(act.getSomething(), entity.getSomething());
	}
	
	@Test
	public void testLocalDateTimeKey() {
		EntityWithDateTimeKey k = new EntityWithDateTimeKey();
		k.setId(new LocalDateTime());
		k.setSomething("qwer");
		
		mgr.put(k);
		mgr.flush();
		
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		EntityWithDateTimeKey entity = mgr2.find(EntityWithDateTimeKey.class, k.getId());
		Assert.assertEquals(k.getSomething(), entity.getSomething());
	}
}
