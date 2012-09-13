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
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.RowKey;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;

public class TestJoins {

	
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
		
		
	}

	
	private void compareKeys(Cursor<IndexColumnInfo> cursor, ViewInfo viewAct, ViewInfo viewAcc, String expectedKey, String expectedAccKey) {
		IndexColumnInfo info = cursor.getCurrent();
		RowKey keyForActivity = info.getKeyForView(viewAct);
		RowKey keyForAccount = info.getKeyForView(viewAcc);

		String key = keyForActivity.getKeyAsString();
		String keyAcc = keyForAccount.getKeyAsString();
		Assert.assertEquals(expectedKey, key);
		Assert.assertEquals(expectedAccKey, keyAcc);
	}

	private void setupRecords() {
		Account acc1 = new Account();
		acc1.setId("acc1");
		acc1.setIsActive(false);
		mgr.put(acc1);
		
		Account acc2 = new Account();
		acc2.setId("acc2");
		acc2.setIsActive(true);
		mgr.put(acc2);
		
		Account acc3 = new Account();
		acc3.setId("acc3");
		acc3.setIsActive(false);
		mgr.put(acc3);
		
		Activity act1 = new Activity();
		act1.setId("act1");
		act1.setAccount(acc1);
		act1.setNumTimes(10);
		mgr.put(act1);
		
		Activity act2 = new Activity();
		act2.setId("act2");
		act2.setAccount(acc1);
		act2.setNumTimes(20);
		mgr.put(act2);

		Activity act3 = new Activity();
		act3.setId("act3");
		act3.setAccount(acc2);
		act3.setNumTimes(10);
		mgr.put(act3);
		
		Activity act4 = new Activity();
		act4.setId("act4");
		act4.setAccount(acc2);
		act4.setNumTimes(20);
		mgr.put(act4);
		
		Activity act5 = new Activity();
		act5.setId("act5");
		act5.setNumTimes(10);
		mgr.put(act5);
		
		Activity act6 = new Activity();
		act6.setId("act6");
		act6.setNumTimes(20);
		mgr.put(act6);

		Activity act7 = new Activity();
		act7.setId("act7");
		act7.setAccount(acc1);
		act7.setNumTimes(10);
		mgr.put(act7);
		
		mgr.flush();
	}

}
