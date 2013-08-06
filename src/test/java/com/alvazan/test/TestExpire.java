package com.alvazan.test;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.ExpiringEntity;

public class TestExpire {

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
	public void testPersistWithExpire() throws InterruptedException {
		ExpiringEntity entity = new ExpiringEntity();
		entity.setId(50);
		entity.setName("test1");
		entity.setTtl(50000);
		mgr.put(entity);
		mgr.flush();

		// read entity back
		mgr.clear();
		entity = mgr.find(ExpiringEntity.class, 50L);
		assertNotNull("Entity not persisted", entity);
	}

	@Test
	public void testTTLReadBack() {
		ExpiringEntity entity = new ExpiringEntity();
		entity.setId(50);
		entity.setName("test1");
		entity.setTtl(50000);
		mgr.put(entity);
		mgr.flush();

		// read entity back
		mgr.clear();
		entity = mgr.find(ExpiringEntity.class, 50L);
		assertEquals("TTL not read back correctly", 50000, entity.getTtl());
	}

	@Test
	public void testExpireEntity() throws InterruptedException {
		ExpiringEntity entity = new ExpiringEntity();
		entity.setId(50);
		entity.setName("test1");
		entity.setTtl(5);
		mgr.put(entity);
		mgr.flush();

		// wait for entity to expire
		Thread.sleep(6000);
		mgr.clear();
		entity = mgr.find(ExpiringEntity.class, 50L);
		assertNull("Entity not expired", entity);
	}
}
