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
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnCommonMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedColumn;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.test.db.Child;
import com.alvazan.test.db.User;

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
		other.clearDatabase(true);
	}
	
	//In one case, we added this String.long.String a few times and our in-memory version saw different values as being the same
	//due to the Utf8Comparator being not so correct in that it translated some different long values back to the same string utf 8 value
	@Test
	public void testRawComposite() {
		NoSqlTypedSession s = mgr.getTypedSession();
		String cf = "TimeSeriesData";
		BigInteger id = BigInteger.valueOf(98);
		TypedRow row = s.createTypedRow(cf);
		row.setRowKey(id);

		createColumn(row, "6e756d526f6f6d73", (byte) 1);
		createColumn(row, "6275696c64696e67", (byte) 2);
		createColumn(row, "6e616d65", (byte) 3);
		createColumn(row, "5f686973746f72790000013caaa1b98d5f7374617465", (byte) 4);
		createColumn(row, "5f686973746f72790000013caaa1b98d6275696c64696e67", (byte) 5);
		createColumn(row, "5f686973746f72790000013caaa1b98d6e756d526f6f6d73", (byte) 6);
		createColumn(row, "5f686973746f72790000013caaa1b9ba6275696c64696e67", (byte) 7);
		createColumn(row, "5f686973746f72790000013caaa1b9ba6e756d526f6f6d73", (byte) 8);
		createColumn(row, "6e756d526f6f6d73", (byte) 0);
		
		s.put(cf, row);
		s.flush();
		
		TypedRow result = s.find(cf, id);
		Assert.assertEquals(8, result.getColumnsAsColl().size());
	}
	
	private void createColumn(TypedRow row, String hex, byte value) {
		byte[] name = StandardConverters.convertFromString(byte[].class, hex);
		row.addColumn(name, new byte[] { value} , null);
	}

	//@Test
	public void testAdvancedJoinClause() {
		//let's add some good data to the table for the join stuff....
		fillInData();
		
		NoSqlTypedSession s = mgr.getTypedSession();
		
		Cursor<KeyValue<TypedRow>> cursor = s.createQueryCursor("select * FROM User as u left join u.age Child as c on u.age = c.age", 500).getPrimaryViewCursor();

		int counter = 0;
		while(cursor.next()) {
			counter++;
		}
		Assert.assertEquals(4, counter);
	}

	private void fillInData() {
		User user1 = new User();
		user1.setAge(5);
		user1.setName("dean");
		mgr.put(user1);
		
		Child child1 = new Child();
		child1.setAge(5);
		child1.setName("bob1");
		mgr.put(child1);
		
		Child child2 = new Child();
		child2.setAge(5);
		child2.setName("dean2");
		mgr.put(child2);
		
		Child child3 = new Child();
		child3.setAge(6);
		child3.setName("none");
		mgr.put(child3);
		
		User user2 = new User();
		user2.setAge(8);
		user2.setName("joe");
		mgr.put(user2);
		
		User user3 = new User();
		user3.setAge(8);
		user3.setName("joe2");
		mgr.put(user3);
		
		Child child4 = new Child();
		child4.setAge(8);
		child4.setName("joe3");
		mgr.put(child4);
		
		User user4 = new User();
		user4.setAge(10);
		user4.setName("nochild");
		
		mgr.put(user4);
		
		Child child5 = new Child();
		child5.setAge(20);
		child5.setName("someone");
		mgr.put(child5);
		
		mgr.flush();
	}
	
	@Test
	public void testBasicChangeToIndex() {
		log.info("testBasicChangeToIndex");
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
	}

	//Okay, let's test some edge cases
	//1. save value of zero for Decimal - returns 0
	//2. save value of null - returns null
	//3. save value of no column exists - returns no column
	//4. save value of 0 length byte array - returns null (use no column to represent null and column with null to represent 0 length)
	//5. save value of 0 length string - returns null (use no column to represent null and column with null to represent 0 length)
	//6. save value of ZERO for Integer - returns 0
	@Test
	public void testZeroVersusNullVersusOther() {
		//Because we can save a name column with no value, can we save a name
		//column with a value of 0?  
		log.info("testTimeSeries");

		NoSqlTypedSession s = mgr.getTypedSession();
		
		//SAVE the value ZERO
		String cf = "TimeSeriesData";
		TypedRow row1 = s.createTypedRow(cf);
		row1.setRowKey(BigInteger.valueOf(25));
		row1.addColumn("temp", new BigDecimal(0));
		row1.addColumn("someName", "dean");
		s.put(cf, row1);

		//SAVE a null value
		TypedRow row2 = s.createTypedRow(cf);
		row2.setRowKey(BigInteger.valueOf(26));
		row2.addColumn("temp", null);
		row2.addColumn("someName", "dean");
		s.put(cf, row2);

		//SAVE with NO column
		TypedRow row3 = s.createTypedRow(cf);
		row3.setRowKey(BigInteger.valueOf(27));
		row3.addColumn("someName", "dean");
		s.put(cf, row3);

		byte[] name = new byte[] {1,2,3,4};
		//SAVE with zero length byte array
		TypedRow row4 = s.createTypedRow(cf);
		row4.setRowKey(BigInteger.valueOf(28));
		row4.addColumn(name, new byte[0], null);
		row4.addColumn("someName", "dean");
		s.put(cf, row4);
		
		//SAVE with zero length byte array
		TypedRow row5 = s.createTypedRow(cf);
		row5.setRowKey(BigInteger.valueOf(29));
		row5.addColumn("other", "");
		row5.addColumn("someName", "dean");
		s.put(cf, row5);

		//SAVE zero for int
		TypedRow row6 = s.createTypedRow(cf);
		row6.setRowKey(BigInteger.valueOf(30));
		row6.addColumn("other", 0);
		row6.addColumn("nullInt", null);
		row6.addColumn("someName", "dean");
		s.put(cf, row6);
		
		s.flush();
		
		//NOW, let's find the row we put
		TypedRow result1 = s.find(cf, row1.getRowKey());
		TypedRow result2 = s.find(cf, row2.getRowKey());
		TypedRow result3 = s.find(cf, row3.getRowKey());
		TypedRow result4 = s.find(cf, row4.getRowKey());
		TypedRow result5 = s.find(cf, row5.getRowKey());
		TypedRow result6 = s.find(cf, row6.getRowKey());
		
		TypedColumn column1 = result1.getColumn("temp");
		Assert.assertNotNull(column1);
		Object val = column1.getValue();
		Assert.assertEquals(new BigDecimal(0), val);
		
		TypedColumn column2 = result2.getColumn("temp");
		Assert.assertNotNull(column2);		
		Object val2 = column2.getValue();
		Assert.assertNull(val2);
		
		TypedColumn column3 = result3.getColumn("temp");
		Assert.assertNull(column3);

		TypedColumn column4 = result4.getColumn(name);
		Assert.assertNotNull(column4);		
		byte[] valueRaw = column4.getValueRaw();
		Assert.assertNull(valueRaw);
		
		TypedColumn column5 = result5.getColumn("other");
		Assert.assertNotNull(column5);
		String value = (String) column5.getValue(String.class);
		Assert.assertEquals(null, value);
		
		TypedColumn column6 = result6.getColumn("other");
		Assert.assertNotNull(column6);
		int value6 = (Integer) column6.getValue(Integer.class);
		Assert.assertEquals(0, value6);
		
		TypedColumn column6b = result6.getColumn("nullInt");
		Integer val6b = column6b.getValue(Integer.class);
		Assert.assertNull(val6b);
	}
	
	@Test
	public void testRemoveColumn() {
		NoSqlTypedSession s = mgr.getTypedSession();
		
		String cf = "TimeSeriesData";
		TypedRow row1 = s.createTypedRow(cf);
		row1.setRowKey(BigInteger.valueOf(25));
		row1.addColumn("temp", new BigDecimal(667));
		row1.addColumn("someName", "dean");
		row1.addColumn("notInSchema", "hithere");
		s.put(cf, row1);
		
		s.flush();
		
		TypedRow result1 = s.find(cf, row1.getRowKey());
		TypedColumn col = result1.getColumn("temp");
		Assert.assertEquals(new BigDecimal(667), col.getValue());
		
		TypedColumn col2 = result1.getColumn("notInSchema");
		String value = col2.getValue(String.class);
		Assert.assertEquals("hithere", value);
		
		result1.removeColumn("temp");
		result1.removeColumn("notInSchema");
		
		s.put(cf, result1);
		s.flush();
		
		TypedRow result2 = s.find(cf, row1.getRowKey());
		
		TypedColumn col4 = result2.getColumn("notInSchema");
		Assert.assertNull(col4);
		
		TypedColumn col3 = result2.getColumn("temp");
		Assert.assertNull(col3);
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
		meta.setup(null, entityName, false, false);
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
