package com.alvazan.test;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.exc.StorageMissingEntitesException;
import com.alvazan.orm.api.exc.TooManyResultException;
import com.alvazan.orm.api.exc.TypeMismatchException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;
import com.alvazan.test.db.PartAccount;

public class TestIndexes {

	private static final Logger log = LoggerFactory.getLogger(TestIndexes.class);
	
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
	public void testFailureOnTypeMismatch() {
		Activity act = new Activity();
		act.setName("hello");
		act.setUniqueColumn("notunique");
		act.setNumTimes(5);
		mgr.put(act);
		Activity act2 = new Activity();
		act2.setUniqueColumn(act.getUniqueColumn());
		act2.setName("hello");
		act2.setNumTimes(4);
		mgr.put(act2);
		
		Query<Activity> query = mgr.createNamedQuery(Activity.class, "findBetween");
		
		double from = 100;
		try {
			query.setParameter("begin", from);
			Assert.fail("Should have throw TypeMismatchException and did not");
		} catch(TypeMismatchException e) {
			log.info("This is expected to fail due to type mismatch");
		}
		
		try {
			query.setParameter("noExistKey", 200);
			Assert.fail("Should have thrown IllegalArgument since noExistKey is not a parameter in this query");
		} catch(IllegalArgumentException e) {
			log.info("this is expected");
		}
	}
	
	@Test
	public void testFailureOnGetSingleResultAndSuccess() {
		//Activity has null reference to account
		Activity act = new Activity();
		act.setName("hello");
		act.setUniqueColumn("notunique");
		act.setNumTimes(5);
		mgr.put(act);
		
		Activity act2 = new Activity();
		act2.setUniqueColumn(act.getUniqueColumn());
		act2.setName("hello");
		act2.setNumTimes(4);
		mgr.put(act2);

		Activity act3 = new Activity();
		act3.setUniqueColumn("isunique");
		act3.setName("hellossss");
		act3.setNumTimes(8);
		mgr.put(act3);
		
		mgr.flush();
		
		try {
			Activity.findSingleResult(mgr, act.getUniqueColumn());
			Assert.fail("IT should throw exception and does not");
		} catch(TooManyResultException e) {
			log.info("yeah, we pass the test");
		}
		
		Activity activity = Activity.findSingleResult(mgr, act3.getUniqueColumn());
		Assert.assertEquals(act3.getNumTimes(), activity.getNumTimes());
		
		Activity activityNull = Activity.findSingleResult(mgr, "notinThere");
		Assert.assertNull(activityNull);
	}
	
	@Test
	public void testTwoQueriesSameNameDifferentEntitiesAllowed() {
		//Account has the same name as a query in Activity which IS allowed in our implementation
		Account acc = new Account();
		acc.setName("tempxxxxx");
		acc.setUsers(9.33333f);
		mgr.put(acc);
		Account acc2 = new Account();
		acc2.setName("xyz");
		acc2.setUsers(3.1f);
		mgr.put(acc2);
		Account acc3 = new Account();
		acc3.setName(acc2.getName());
		acc3.setUsers(2.9f);
		mgr.put(acc3);
		
		Activity act = new Activity();
		mgr.put(act);
		
		mgr.flush();
		
		List<Account> all2 = Account.findAll(mgr);
		Assert.assertEquals(3, all2.size());
		
		List<Activity> all3 = Activity.findAll(mgr, 100);
		Assert.assertEquals(1, all3.size());
	}
	
	
	
	@Test
	@SuppressWarnings("unchecked")
	public void testIndexedButNotInNoSqlDatabaseList() {
		PartAccount acc = new PartAccount();
		acc.setBusinessName("abc");
		mgr.put(acc);
		PartAccount acc2 = new PartAccount();
		acc2.setBusinessName("dean");
		mgr.put(acc2);
		
		mgr.flush();
		
		//Here we have to go raw and update the index ourselves with another fake PartAccount that does
		//not exist
		NoSqlSession session = mgr.getSession();
		DboTableMeta table = mgr.find(DboTableMeta.class, "PartAccount");
		DboColumnMeta colMeta = table.getColumnMeta("businessName");
		ScanInfo info = ScanInfo.createScanInfo(colMeta, null, null);
		IndexColumn col = new IndexColumn();
		col.setColumnName("businessName");
		String key = "nonexistpk";
		byte[] pk = StandardConverters.convertToBytes(key);
		byte[] value = StandardConverters.convertToBytes("DeansCoolBusiness");
		col.setIndexedValue(value);
		col.setPrimaryKey(pk);
		session.persistIndex("PartAccount", info.getIndexColFamily(), info.getRowKey(), col);
		
		mgr.flush();
		
		Iterable<KeyValue<PartAccount>> all = PartAccount.findAll2(mgr);
		
		KeyValue<PartAccount> kVal = null;
		for(KeyValue<PartAccount> k : all) {
			if(k.getKey().equals(key))
				kVal = k;
		}
		
		try {
			kVal.getValue();
			Assert.fail("This keyValue has no real value so should throw exception");
		} catch(RowNotFoundException e) {
			log.info("this should occur");
		}
		
		
		//NOTE: Account3 was NOT PUT in the database(or you could say removed but index not updated yet)
		try {
			PartAccount.findAll(mgr);
			Assert.fail("It should fail since account 3 is not in storage");
		} catch(StorageMissingEntitesException e) {
			List<PartAccount> foundAccounts = formList(e.getFoundElements());
			Assert.assertEquals(2, foundAccounts.size());
		}
	}

	private List<PartAccount> formList(Iterable<PartAccount> foundElements) {
		List<PartAccount> all = new ArrayList<PartAccount>();
		for(PartAccount p : foundElements) {
			all.add(p);
		}
		return all;
	}


}
