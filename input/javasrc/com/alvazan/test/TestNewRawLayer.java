package com.alvazan.test;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.spi1.NoSqlTypedSession;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi2.TypedColumn;
import com.alvazan.orm.api.spi2.TypedRow;
import com.alvazan.orm.api.spi2.meta.DboDatabaseMeta;
import com.alvazan.orm.layer1.typed.NoSqlTypedSessionImpl;

public class TestNewRawLayer {

	private static final Logger log = LoggerFactory.getLogger(TestNewRawLayer.class);
	private static NoSqlEntityManagerFactory factory;
	private DboDatabaseMeta db;
	private NoSqlEntityManager mgr;
	
	@BeforeClass
	public static void setup() {
		factory = FactorySingleton.createFactoryOnce();
	}
	
	@Before
	public void createEntityManager() {
		mgr = factory.createEntityManager();
		db = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
	}
	
	@After
	public void clearDatabase() {

	}
	
	@Test
	public void testBasicChangeToIndex() {
		NoSqlSession session = mgr.getSession();

		//A simple trick for now so we can test this layer
		NoSqlTypedSession s = new NoSqlTypedSessionImpl();
		s.setRawSession(session);
		s.setMetaInfo(db);
		
		String cf = "User";
		String id = "someid";
		TypedRow<String> row = createUser(id, "dean", "hiller");
		s.put(cf, row);
		s.flush();
		
		//NOW, let's find the row we put
		TypedRow<String> result = s.find(cf, id);
		Assert.assertEquals(id, result.getRowKey());
		Assert.assertEquals(row.getColumn("name").getValue(), result.getColumn("name").getValue());
		Assert.assertEquals(row.getColumn("lastName").getValue(), result.getColumn("lastName").getValue());
	}

	@Test
	public void testTimeSeries() {
		NoSqlSession session = mgr.getSession();
		
		//A simple trick for now so we can test this layer
		NoSqlTypedSession s = new NoSqlTypedSessionImpl();
		s.setRawSession(session);
		s.setMetaInfo(db);

		String cf = "TimeSeriesData";
		TypedRow<BigInteger> row = new TypedRow<BigInteger>();
		row.setRowKey(BigInteger.valueOf(25));
		row.addColumn(new TypedColumn("temp", new BigDecimal(55.6)));
		row.addColumn(new TypedColumn("someName", "dean"));
		
		s.put(cf, row);
		s.flush();
		
		//NOW, let's find the row we put
		TypedRow<BigInteger> result = s.find(cf, row.getRowKey());
		Assert.assertEquals(row.getRowKey(), result.getRowKey());
		Assert.assertEquals(row.getColumn("temp").getValue(), result.getColumn("temp").getValue());
		Assert.assertEquals(row.getColumn("someName").getValue(), result.getColumn("someName").getValue());
		
		
	}
	
	private TypedRow<String> createUser(String key, String name, String lastname) {
		TypedRow<String> row = new TypedRow<String>();
		row.setRowKey(key);
		row.addColumn(new TypedColumn("name", name));
		row.addColumn(new TypedColumn("lastName", lastname));
		return row;
	}
	
}
