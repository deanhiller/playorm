package com.alvazan.test;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Marker;

public class TestMarker {

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
	public void testInsertAndLoadMarker() {
		Marker m = new Marker();
		m.setId("one");
		
		// persist marker
		mgr.put(m);
		mgr.flush();
		
		// check if marker was saved
		mgr.clear();
		assertNotNull("marker object not persisted into database", mgr.find(Marker.class, "one"));
	}
}
