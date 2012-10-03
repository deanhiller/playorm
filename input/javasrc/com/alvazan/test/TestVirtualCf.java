package com.alvazan.test;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z3api.QueryResult;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnCommonMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnToOneMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.test.db.Activity;
import com.alvazan.test.db.PartitionedSingleTrade;
import com.alvazan.test.db.TimeSeriesData;

public class TestVirtualCf {

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
	public void testRawStuff() {
		DboTableMeta fkToTable = new DboTableMeta();
		fkToTable.setup("Owner", "ourstuff");
		DboColumnIdMeta id = new DboColumnIdMeta();
		id.setup(fkToTable, "id", String.class, true);
		DboColumnCommonMeta col1 = new DboColumnCommonMeta();
		col1.setup(fkToTable, "name", String.class, false, false);
		
		mgr.put(fkToTable);
		mgr.put(id);
		mgr.put(col1);
		
		DboTableMeta meta = new DboTableMeta();
		meta.setup("MyRaceCar", "ourstuff");
		DboColumnIdMeta idMeta = new DboColumnIdMeta();
		idMeta.setup(meta, "id", String.class, true);
		DboColumnToOneMeta toOne = new DboColumnToOneMeta();
		toOne.setup(meta, "carOwner", fkToTable, false, false);
		
		mgr.put(meta);
		mgr.put(idMeta);
		mgr.put(toOne);
		mgr.flush();
		
		NoSqlTypedSession s = mgr.getTypedSession();

		TypedRow row2 = s.createTypedRow("Owner");
		row2.setRowKey("myoneid");
		row2.addColumn("name", "dean");
		
		s.put("Owner", row2);

		byte[] temp = new byte[2];
		temp[0] = 23;
		temp[1] = 24;
		TypedRow row = s.createTypedRow("MyRaceCar");
		row.setRowKey("myoneid");
		row.addColumn("carOwner", row2.getRowKey());
		
		s.put("MyRaceCar", row);
		
		s.flush();
		
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		NoSqlTypedSession s2 = mgr2.getTypedSession();
		
		s2.remove("Owner", row2);
		s2.flush();
		
		TypedRow result = s2.find("MyRaceCar", row.getRowKey());
		Assert.assertNotNull(result);
		TypedRow result2 = s2.find("Owner", row2.getRowKey());
		Assert.assertNull(result2);
		
		Object fk = result.getColumn("carOwner").getValue();
		Object rowId = row2.getRowKey();
		Assert.assertEquals(rowId, fk);
		
		QueryResult qResult = s2.createQueryCursor("select * from MyRaceCar", 50);
		Cursor<KeyValue<TypedRow>> primView = qResult.getPrimaryViewCursor();
		Assert.assertTrue(primView.next());
		KeyValue<TypedRow> current = primView.getCurrent();
		TypedRow resultRow = current.getValue();
		Assert.assertEquals(row.getRowKey(), resultRow.getRowKey());
		
		Cursor<List<TypedRow>> cursor = qResult.getAllViewsCursor();
		Assert.assertTrue(cursor.next());
		List<TypedRow> theRow = cursor.getCurrent();
		Assert.assertEquals(1, theRow.size());
		TypedRow myRow = theRow.get(0);
		Assert.assertEquals(row.getRowKey(), myRow.getRowKey());
		
		QueryResult rResult = s2.createQueryCursor("select * from Owner", 50);
		Assert.assertFalse(rResult.getCursor().next());
	}
	
	@Test
	public void testWriteRead() {
		//let's use TWO entities that share the same columnFamily AND use the same key in both to make sure
		//the prefix stuff works just fine...
		
		Activity act1 = new Activity("myid");
		act1.setName("myname");
		mgr.put(act1);
		
		PartitionedSingleTrade trade = new PartitionedSingleTrade();
		trade.setId("myid");
		trade.setNumber(89);
		mgr.put(trade);

		//throw an a guy with Long key types as well...
		TimeSeriesData d = new TimeSeriesData();
		d.setKey(897L);
		d.setSomeName("qwer");
		mgr.put(d);
		mgr.flush();
		
		//unfortunately, the two rows are written as one (ie. MERGED) so
		//to really TEST this out, we remove the trade row to make sure we still have the Activity
		//row
		mgr.remove(trade);
		mgr.flush();
		
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		
		PartitionedSingleTrade r = mgr2.find(PartitionedSingleTrade.class, trade.getId());
		Assert.assertNull(r);
		
		Activity act = mgr2.find(Activity.class, act1.getId());
		Assert.assertNotNull(act);
		Assert.assertEquals(act1.getName(), act.getName());
		
		TimeSeriesData d2 = mgr2.find(TimeSeriesData.class, d.getKey());
		Assert.assertEquals(d.getSomeName(), d2.getSomeName());
	}
	
}
