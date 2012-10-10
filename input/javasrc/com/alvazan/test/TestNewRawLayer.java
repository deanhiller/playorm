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
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnCommonMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;

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

	}
	
	@Test
	public void testBasicChangeToIndex() {
		log.info("testBasicChangeToIndex");
		try {
		NoSqlTypedSession s = mgr.getTypedSession();
		
		String cf = "User";
		String id = "someid";
		TypedRow row = s.createTypedRow(cf);
		createUser(row, id, "dean", "hiller");
		s.put(cf, row);
		s.flush();
		
		//NOW, let's find the row we put
		TypedRow result = s.find(cf, id);
		Assert.assertEquals(id, result.getRowKey());
		Assert.assertEquals(row.getColumn("name").getValue(), result.getColumn("name").getValue());
		Assert.assertEquals(row.getColumn("lastName").getValue(), result.getColumn("lastName").getValue());
		} finally {
		log.info("DONE test BasicChangeToIndex");
		}
	}
	
	@Test
	public void testTimeSeries() {
		log.info("testTimeSeries");

		NoSqlTypedSession s = mgr.getTypedSession();
		
		String cf = "TimeSeriesData";
		TypedRow row = s.createTypedRow(cf);
		row.setRowKey(BigInteger.valueOf(25));
		row.addColumn("temp", new BigDecimal(55.6));
		row.addColumn("someName", "dean");
		
		s.put(cf, row);
		s.flush();
		
		//NOW, let's find the row we put
		TypedRow result = s.find(cf, row.getRowKey());
		Assert.assertEquals(row.getRowKey(), result.getRowKey());
		Assert.assertEquals(row.getColumn("temp").getValue(), result.getColumn("temp").getValue());
		Assert.assertEquals(row.getColumn("someName").getValue(), result.getColumn("someName").getValue());
		
		Cursor<KeyValue<TypedRow>> rowsIter = s.createQueryCursor("select s FROM TimeSeriesData as s where s.key = 25", 500).getPrimaryViewCursor();
		rowsIter.next();
		KeyValue<TypedRow> keyValue = rowsIter.getCurrent();
		TypedRow theRow = keyValue.getValue();
		Assert.assertEquals(row.getRowKey(), theRow.getRowKey());
		Assert.assertEquals(row.getColumn("temp").getValue(), theRow.getColumn("temp").getValue());

		//Testing a negative value in the SQL here
		Cursor<KeyValue<TypedRow>> rows2 = s.createQueryCursor("select s FROM TimeSeriesData as s where s.key > -25", 500).getPrimaryViewCursor();
		rows2.next();
		KeyValue<TypedRow> keyValue2 = rows2.getCurrent();
		TypedRow theRow2 = keyValue2.getValue();
		Assert.assertEquals(row.getRowKey(), theRow2.getRowKey());
		Assert.assertEquals(row.getColumn("temp").getValue(), theRow2.getColumn("temp").getValue());
	}
	
	private TypedRow createUser(TypedRow row, String key, String name, String lastname) {
		row.setRowKey(key);
		row.addColumn("name", name);
		row.addColumn("lastName", lastname);
		return row;
	}

	@Test
	public void testOrmLayerMetaSaved() {
		NoSqlEntityManagerFactory factory = FactorySingleton.createFactoryOnce();
		NoSqlEntityManager mgr = factory.createEntityManager();

		DboDatabaseMeta database = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
		DboTableMeta table = database.getMeta("Activity");
		DboColumnMeta columnMeta = table.getColumnMeta("account");
		DboColumnToOneMeta toOne = (DboColumnToOneMeta) columnMeta;
		Assert.assertEquals("id", toOne.getFkToColumnFamily().getIdColumnMeta().getColumnName());
		
	}
	
	@Test
	public void testBasic() {
		DboDatabaseMeta metaDb = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
		
		addMetaClassDbo(metaDb, "MyEntity", "theid", "cat", "mouse", "dog");
		addMetaClassDbo(metaDb, "OtherEntity", "id", "dean", "declan", "pet", "house");

		mgr.flush();

		NoSqlTypedSession session = mgr.getTypedSession();
		
		String sql = "select * FROM MyEntity as e WHERE e.cat = \"deano\"";

		TypedRow typedRow = session.createTypedRow("MyEntity");

		typedRow.addColumn("cat", "deano");
		typedRow.setRowKey("dean1");
		
		session.put("MyEntity", typedRow);
		
		session.flush();
		
		Iterable<KeyValue<TypedRow>> rowsIterable = session.createQueryCursor(sql, 50).getPrimaryViewIter();
		int counter = 0;
		for(@SuppressWarnings("unused") KeyValue<TypedRow> k : rowsIterable) {
			counter++;
		}
		Assert.assertEquals(1, counter);
	}

	private DboTableMeta addMetaClassDbo(DboDatabaseMeta map, String entityName, String idField, String ... fields) {
		DboTableMeta meta = new DboTableMeta();
		meta.setup(null, entityName, false);
		map.addMetaClassDbo(meta);
		
		DboColumnIdMeta idMeta = new DboColumnIdMeta();
		idMeta.setup(meta, idField, String.class, true);
		mgr.put(idMeta);
		
		for(String field : fields) {
			DboColumnCommonMeta fieldDbo = new DboColumnCommonMeta();
			fieldDbo.setup(meta, field, String.class, true, false);
			mgr.put(fieldDbo);
		}
		mgr.put(meta);
		return meta;
	}
	
}
