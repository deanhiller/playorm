package com.alvazan.test;

import java.util.ArrayList;
import java.util.List;

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
import com.alvazan.orm.api.z3api.QueryResult;
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.IndexPoint;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;

public class TestJoins {

	private static final Logger log = LoggerFactory.getLogger(TestJoins.class);
	
	private static NoSqlEntityManagerFactory factory;
	private NoSqlEntityManager mgr;

	@BeforeClass
	public static void setup() {
		factory = FactorySingleton.createFactoryOnce();
	}
	
	@Before
	public void createEntityManager() {
		mgr = factory.createEntityManager();
		setupRecords();
	}
	@After
	public void clearDatabase() {
		NoSqlEntityManager other = factory.createEntityManager();
		other.clearDatabase(true);
	}
	
	@Test
	public void testViewIndex() {
		NoSqlTypedSession s = mgr.getTypedSession();
		
		//Here we can pull from a single index to view the index itself. 
		//Supply the column family(Activity) the column that is indexed(numTimes)
		//IF a table is partitioned one way, you can add a partitionId
		//IF a table is partitioned multiple ways, you MUST supply partitionBy and you can supply a partitionId or null for null partition
		Cursor<IndexPoint> cursor = s.indexView("Activity", "numTimes", null, null);
		
		List<IndexPoint> points = new ArrayList<IndexPoint>();
		while(cursor.next()) {
			IndexPoint pt = cursor.getCurrent();
			points.add(pt);
		}
		
		log.info("All index values together are="+points);
		Assert.assertEquals("10", points.get(0).getIndexedValueAsString());
		Assert.assertEquals("act1", points.get(0).getKey());
	}
	
	@Test
	public void testInnerJoin() throws InterruptedException {
		NoSqlTypedSession s = mgr.getTypedSession();

		QueryResult result = s.createQueryCursor("select * FROM Activity as e INNER JOIN e.account  as a WHERE e.numTimes < 15 and a.isActive = false", 50);
		List<ViewInfo> views = result.getViews();
		Cursor<IndexColumnInfo> cursor = result.getCursor();

		ViewInfo viewAct = views.get(0);
		ViewInfo viewAcc = views.get(1);
		String alias1 = viewAct.getAlias();
		String alias2 = viewAcc.getAlias();
		Assert.assertEquals("e", alias1);
		Assert.assertEquals("a", alias2);
		
		Assert.assertTrue(cursor.next());
		compareKeys(cursor, viewAct, viewAcc, "act1", "acc1");
		Assert.assertTrue(cursor.next());
		compareKeys(cursor, viewAct, viewAcc, "act7", "acc1");
		Assert.assertFalse(cursor.next());
		
		Cursor<List<TypedRow>> rows = result.getAllViewsCursor();
		
		rows.next();
		List<TypedRow> joinedRow = rows.getCurrent();
		
		TypedRow typedRow = joinedRow.get(0);
		TypedRow theJoinedRow = joinedRow.get(1);
		
		log.info("joinedRow="+joinedRow);
		Assert.assertEquals("e", typedRow.getView().getAlias());
		Assert.assertEquals("act1", typedRow.getRowKeyString());
		Assert.assertEquals("acc1", theJoinedRow.getRowKey());
	}

	//Well, we fail here as postgres returns just two results and does NOT return the results where e.account= null do to the inner join WHERE we do return the
	//results for e.account=null as that is what the user "would" expect since he declared it.
	
//	/**
//	 * This is setup to match postgres result we saw
//	 * @throws InterruptedException
//	 */
//	@Test
//	public void testInnerJoinWithNullClause() throws InterruptedException {
//		NoSqlTypedSession s = mgr.getTypedSession();
//												 "select * FROM Activity as e INNER JOIN e.account as a WHERE e.numTimes < 15 and a.isActive = false"
//		QueryResult result = s.createQueryCursor("select * FROM Activity as e INNER JOIN e.account as a WHERE e.numTimes < 15 and (e.account = null or a.isActive = false)", 50);
//		List<ViewInfo> views = result.getViews();
//		Cursor<IndexColumnInfo> cursor = result.getCursor();
//
//		ViewInfo viewAct = views.get(0);
//		ViewInfo viewAcc = views.get(1);
//		String alias1 = viewAct.getAlias();
//		String alias2 = viewAcc.getAlias();
//		Assert.assertEquals("e", alias1);
//		Assert.assertEquals("a", alias2);
//		
//		Assert.assertTrue(cursor.next());
//		compareKeys(cursor, viewAct, viewAcc, "act1", "acc1");
//		Assert.assertTrue(cursor.next());
//		compareKeys(cursor, viewAct, viewAcc, "act7", "acc1");
//		Assert.assertFalse(cursor.next());
//		
//		Cursor<List<TypedRow>> rows = result.getAllViewsCursor();
//		
//		rows.next();
//		List<TypedRow> joinedRow = rows.getCurrent();
//		
//		TypedRow typedRow = joinedRow.get(0);
//		TypedRow theJoinedRow = joinedRow.get(1);
//		
//		log.info("joinedRow="+joinedRow);
//		Assert.assertEquals("e", typedRow.getView().getAlias());
//		Assert.assertEquals("act1", typedRow.getRowKeyString());
//		Assert.assertEquals("acc1", theJoinedRow.getRowKey());
//	}

	@Test
	public void testJoinViewButNoJoin() throws InterruptedException {
		NoSqlTypedSession s = mgr.getTypedSession();

		QueryResult result = s.createQueryCursor("select * FROM Activity as e WHERE e.numTimes < 15", 50);
		List<ViewInfo> views = result.getViews();
		Assert.assertEquals(1, views.size());
		Cursor<IndexColumnInfo> cursor = result.getCursor();

		ViewInfo viewAct = views.get(0);
		String alias1 = viewAct.getAlias();
		Assert.assertEquals("e", alias1);
		
		Assert.assertTrue(cursor.next());
		compareKeys2(cursor, viewAct, "act1");
		Assert.assertTrue(cursor.next());
		compareKeys2(cursor, viewAct, "act3");
		Assert.assertTrue(cursor.next());
		compareKeys2(cursor, viewAct, "act5");
		Assert.assertTrue(cursor.next());
		compareKeys2(cursor, viewAct, "act7");
		Assert.assertFalse(cursor.next());
		
		Cursor<List<TypedRow>> rows = result.getAllViewsCursor();
		
		rows.next();
		List<TypedRow> joinedRow = rows.getCurrent();
		Assert.assertEquals(1, joinedRow.size());
		
		TypedRow typedRow = joinedRow.get(0);

		log.info("joinedRow="+joinedRow);
		Assert.assertEquals("e", typedRow.getView().getAlias());
		Assert.assertEquals("act1", typedRow.getRowKeyString());
	}
	
	private void compareKeys2(Cursor<IndexColumnInfo> cursor, ViewInfo viewAct,
			String expectedKey) {
		IndexColumnInfo info = cursor.getCurrent();
		IndexPoint keyForActivity = info.getKeyForView(viewAct);
		String key = keyForActivity.getKeyAsString();
		Assert.assertEquals(expectedKey, key);
	}

	@Test
	public void testOuterJoin() throws InterruptedException {
		NoSqlTypedSession s = mgr.getTypedSession();

		QueryResult result = s.createQueryCursor("select * FROM Activity as e LEFT JOIN e.account as a WHERE e.numTimes < 15 and a.isActive = false", 50);
		List<ViewInfo> views = result.getViews();
		Cursor<IndexColumnInfo> cursor = result.getCursor();

		ViewInfo viewAct = views.get(0);
		ViewInfo viewAcc = views.get(1);
		String alias1 = viewAct.getAlias();
		String alias2 = viewAcc.getAlias();
		Assert.assertEquals("e", alias1);
		Assert.assertEquals("a", alias2);
		
		Assert.assertTrue(cursor.next());
		compareKeys(cursor, viewAct, viewAcc, "act1", "acc1");
		Assert.assertTrue(cursor.next());
		compareKeys(cursor, viewAct, viewAcc, "act7", "acc1");
		Assert.assertFalse(cursor.next());
		
		Cursor<List<TypedRow>> rows = result.getAllViewsCursor();
		
		rows.next();
		List<TypedRow> joinedRow = rows.getCurrent();
		
		TypedRow typedRow = joinedRow.get(0);
		TypedRow theJoinedRow = joinedRow.get(1);
		
		log.info("joinedRow="+joinedRow);
		Assert.assertEquals("e", typedRow.getView().getAlias());
		Assert.assertEquals("act1", typedRow.getRowKeyString());
		Assert.assertEquals("acc1", theJoinedRow.getRowKey());
	}
	
	/**
	 * This is setup to match postgres result we saw
	 * @throws InterruptedException
	 */
	@Test
	public void testOuterJoinWithNullClause() throws InterruptedException {
		NoSqlTypedSession s = mgr.getTypedSession();
		
		QueryResult result = s.createQueryCursor("select * FROM Activity as e LEFT JOIN e.account as a WHERE e.numTimes < 15 and (e.account = null or a.isActive = false)", 50);
		List<ViewInfo> views = result.getViews();
		Cursor<IndexColumnInfo> cursor = result.getCursor();

		ViewInfo viewAct = views.get(0);
		ViewInfo viewAcc = views.get(1);
		String alias1 = viewAct.getAlias();
		String alias2 = viewAcc.getAlias();
		Assert.assertEquals("e", alias1);
		Assert.assertEquals("a", alias2);
		
		Assert.assertTrue(cursor.next());
		compareKeys(cursor, viewAct, viewAcc, "act1", "acc1");
		Assert.assertTrue(cursor.next());
		compareKeys(cursor, viewAct, viewAcc, "act5", null);
		Assert.assertTrue(cursor.next());
		compareKeys(cursor, viewAct, viewAcc, "act7", "acc1");
		Assert.assertFalse(cursor.next());
		
		Cursor<List<TypedRow>> rows = result.getAllViewsCursor();
		
		rows.next();
		List<TypedRow> joinedRow = rows.getCurrent();
		
		TypedRow typedRow = joinedRow.get(0);
		TypedRow theJoinedRow = joinedRow.get(1);
		
		log.info("joinedRow="+joinedRow);
		Assert.assertEquals("e", typedRow.getView().getAlias());
		Assert.assertEquals("act1", typedRow.getRowKeyString());
		Assert.assertEquals("acc1", theJoinedRow.getRowKey());
		
	    rows.next();
	    List<TypedRow> joinedRow1 = rows.getCurrent();
	    TypedRow typedRow1 = joinedRow1.get(0);
	    TypedRow theJoinedRow1 = joinedRow1.get(1);
	    log.info("joinedRow1= "+joinedRow1);
	    Assert.assertEquals("e", typedRow1.getView().getAlias());
	    Assert.assertEquals("act5", typedRow1.getRowKeyString());
	    Assert.assertEquals(null, theJoinedRow1.getRowKey());

	    rows.next();
	    List<TypedRow> joinedRow2 = rows.getCurrent();
	    TypedRow typedRow2 = joinedRow2.get(0);
	    TypedRow theJoinedRow2 = joinedRow2.get(1);
	    log.info("joinedRow1= "+joinedRow2);
	    Assert.assertEquals("e", typedRow2.getView().getAlias());
	    Assert.assertEquals("act7", typedRow2.getRowKeyString());
	    Assert.assertEquals("acc1", theJoinedRow2.getRowKey());
	}
	
	private void compareKeys(Cursor<IndexColumnInfo> cursor, ViewInfo viewAct, ViewInfo viewAcc, String expectedKey, String expectedAccKey) {
		IndexColumnInfo info = cursor.getCurrent();
		IndexPoint keyForActivity = info.getKeyForView(viewAct);
		String key = keyForActivity.getKeyAsString();
		
		String keyAcc = null;
		if(expectedAccKey != null) {
			IndexPoint keyForAccount = info.getKeyForView(viewAcc);
			keyAcc = keyForAccount.getKeyAsString();
		}
		
		Assert.assertEquals(expectedKey, key);
		Assert.assertEquals(expectedAccKey, keyAcc);
	}

	private void setupRecords() {
		Account acc1 = new Account("acc1");
		acc1.setIsActive(false);
		mgr.fillInWithKey(acc1);

		
		Account acc2 = new Account("acc2");
		acc2.setIsActive(true);
		mgr.fillInWithKey(acc2);
		
		Account acc3 = new Account("acc3");
		acc3.setIsActive(false);
		mgr.fillInWithKey(acc3);

		
		Activity act1 = new Activity("act1");
		act1.setAccount(acc1);
		act1.setNumTimes(10);
		acc1.addActivity(act1);
		mgr.put(act1);
		
		Activity act2 = new Activity("act2");
		act2.setAccount(acc1);
		act2.setNumTimes(20);
		acc1.addActivity(act2);
		mgr.put(act2);

		Activity act3 = new Activity("act3");
		act3.setAccount(acc2);
		act3.setNumTimes(10);
		acc2.addActivity(act3);
		mgr.put(act3);
		
		Activity act4 = new Activity("act4");
		act4.setId("act4");
		act4.setAccount(acc2);
		act4.setNumTimes(20);
		acc2.addActivity(act4);
		mgr.put(act4);
		
		Activity act5 = new Activity("act5");
		act5.setId("act5");
		act5.setNumTimes(10);
		mgr.put(act5);
		
		Activity act6 = new Activity("act6");
		act6.setId("act6");
		act6.setNumTimes(20);
		mgr.put(act6);

		Activity act7 = new Activity("act7");
		act7.setId("act7");
		act7.setAccount(acc1);
		act7.setNumTimes(10);
		acc1.addActivity(act7);
		mgr.put(act7);
		
		mgr.put(acc1);
		mgr.put(acc2);
		mgr.put(acc3);
		
		mgr.flush();
	}

}
