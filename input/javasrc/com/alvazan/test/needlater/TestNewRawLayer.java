package com.alvazan.test.needlater;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.spi1.NoSqlTypedSession;
import com.alvazan.orm.api.spi1.TypedColumn;
import com.alvazan.orm.api.spi1.TypedRow;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.layer1.typed.NoSqlTypedSessionImpl;
import com.alvazan.test.FactorySingleton;

public class TestNewRawLayer {

	private static final Logger log = LoggerFactory.getLogger(TestNewRawLayer.class);
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
			other.clearDbAndIndexesIfInMemoryType();
		} catch(Exception e) {
			log.warn("Could not clean up properly", e);
		}
	}
	
	//@Test
	public void testBasicChangeToIndex() {
		NoSqlSession session = mgr.getSession();
		
		DboDatabaseMeta db = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
		DboTableMeta meta = db.getMeta("User");
		
		NoSqlTypedSession s = new NoSqlTypedSessionImpl();
		s.setRawSession(session);
		
		TypedRow<String> row = createUser("someid", "dean", "hiller");
		s.persist(meta, row);
		s.flush();
	}

	private TypedRow<String> createUser(String key, String name, String lastname) {
		TypedRow<String> row = new TypedRow<String>();
		row.setRowKey(key);
		row.addColumn(new TypedColumn("name", name));
		row.addColumn(new TypedColumn("lastName", lastname));
		return row;
	}
	
	
}
