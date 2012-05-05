package com.alvazan.test;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.Bootstrap;
import com.alvazan.orm.api.DbTypeEnum;
import com.alvazan.orm.api.Index;
import com.alvazan.orm.api.NoSqlEntityManager;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.StorageMissingEntitesException;
import com.alvazan.orm.api.TooManyResultException;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;

public class TestIndexes {

	private static final Logger log = LoggerFactory.getLogger(TestIndexes.class);
	
	public NoSqlEntityManagerFactory setup() {
		NoSqlEntityManagerFactory factory = Bootstrap.create(DbTypeEnum.IN_MEMORY);
		factory.setup(null, "com.alvazan.test.db");
		return factory;
	}
	
	@Test
	public void testEmpty() {
		log.info("empty so test passes for now");
	}
	
	//@Test
	public void testFailureOnGetSingleResultAndSuccess() {
		NoSqlEntityManagerFactory factory = setup();
		NoSqlEntityManager mgr = factory.createEntityManager();

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
		
		Index<Activity> index = mgr.getIndex(Activity.class, "/activity/byaccount/account1");
		index.addToIndex(act);
		index.addToIndex(act2);
		
		Index<Activity> index2 = mgr.getIndex(Activity.class, "/activity/bysecurity/security1");
		index.addToIndex(act);
		
		mgr.flush();
		
		try {
			Activity.findSingleResult(index, act.getUniqueColumn());
			Assert.fail("IT should throw exception and does not");
		} catch(TooManyResultException e) {
			log.info("yeah, we pass the test");
		}
		
		Activity activity = Activity.findSingleResult(index2, act.getUniqueColumn());
		Assert.assertEquals(act.getNumTimes(), activity.getNumTimes());
		
		Activity activityNull = Activity.findSingleResult(index, "notinThere");
		Assert.assertNull(activityNull);
	}
	
	//@Test
	public void testTwoQueriesSameNameDifferenceEntitiesAllowed() {
		NoSqlEntityManagerFactory factory = setup();
		NoSqlEntityManager mgr = factory.createEntityManager();
		
		//Account has the same name as a query in Activity which IS allowed in our implementation
		Account acc = new Account();
		acc.setName("tempxxxxx");
		acc.setIndexedValue(9.33333f);
		mgr.put(acc);
		Account acc2 = new Account();
		acc2.setName("xyz");
		acc2.setIndexedValue(3.1f);
		mgr.put(acc2);
		Account acc3 = new Account();
		acc3.setName(acc2.getName());
		acc3.setIndexedValue(2.9f);
		mgr.put(acc3);
		
		Index<Account> index = mgr.getIndex(Account.class, "/someindex");
		index.addToIndex(acc);
		index.addToIndex(acc2);
		index.addToIndex(acc3);
		
		Index<Account> index2 = mgr.getIndex(Account.class, "/otherindex");
		index.addToIndex(acc2);
		
		mgr.flush();
		
		List<Account> results = Account.findBetween(index, 2.555f, 3.444f);
		Assert.assertEquals(2, results.size());
		Assert.assertEquals(acc2.getName(), results.get(0).getName());
		
		List<Account> all = Account.findAll(index);
		Assert.assertEquals(3, all.size());
		
		List<Account> all2 = Account.findAll(index2);
		Assert.assertEquals(1, all2.size());
	}
	
	//@Test
	public void testBooleanWithAndClause() {
		NoSqlEntityManagerFactory factory = setup();
		NoSqlEntityManager mgr = factory.createEntityManager();
		
		Account acc = new Account();
		acc.setName("abc");
		acc.setIsActive(true);
		mgr.put(acc);
		Account acc2 = new Account();
		acc2.setName("dean");
		acc2.setIsActive(false);
		mgr.put(acc2);
		Account acc3 = new Account();
		acc3.setName("dean");
		acc3.setIsActive(true);
		mgr.put(acc3);
		Account acc4 = new Account();
		acc4.setName("dean");
		acc4.setIsActive(true);
		mgr.put(acc4);		
		Account acc5 = new Account();
		acc5.setName("dean");
		acc5.setIsActive(null);
		mgr.put(acc5);
		
		Index<Account> index = mgr.getIndex(Account.class, "/someindex");
		index.addToIndex(acc);
		index.addToIndex(acc2);
		index.addToIndex(acc3);		
		index.addToIndex(acc4);
		index.addToIndex(acc5);
	
		mgr.flush();
		
		List<Account> activeList = Account.findAnd(index, "dean", true);
		Assert.assertEquals(2, activeList.size());
		
		List<Account> nullList = Account.findAnd(index, "dean", null);
		Assert.assertEquals(1, nullList.size());
		
		List<Account> orList = Account.findOr(index, "dean", true);
		Assert.assertEquals(5, orList.size());
	}
	
	//@Test
	@SuppressWarnings("unchecked")
	public void testIndexedButNotInNoSqlDatabaseList() {
		NoSqlEntityManagerFactory factory = setup();
		NoSqlEntityManager mgr = factory.createEntityManager();
		
		Account acc = new Account();
		acc.setName("abc");
		acc.setIsActive(true);
		mgr.put(acc);
		Account acc2 = new Account();
		acc2.setName("dean");
		acc2.setIsActive(false);
		mgr.put(acc2);
		Account acc3 = new Account();
		acc3.setName("dean");
		acc3.setIsActive(true);
		
		Index<Account> index = mgr.getIndex(Account.class, "/someindex");
		index.addToIndex(acc);
		index.addToIndex(acc2);
		index.addToIndex(acc3);	

		mgr.flush();
		
		//NOTE: Account3 was NOT PUT in the database(or you could say removed but index not updated yet)
		try {
			Account.findAll(index);
			Assert.fail("It should fail since account 3 is not in storage");
		} catch(StorageMissingEntitesException e) {
			List<Account> foundAccounts = e.getFoundElements();
			Assert.assertEquals(2, foundAccounts.size());
		}
	}
	
	public void testSeparateIndexes() {
		NoSqlEntityManagerFactory factory = setup();
		NoSqlEntityManager mgr = factory.createEntityManager();

		//Activity has null reference to account
		Activity act = new Activity();
		act.setName("hello");
		act.setNumTimes(5);
		mgr.put(act);
		Activity act2 = new Activity();
		act2.setName("hello");
		act2.setNumTimes(4);
		mgr.put(act2);
		
		Index<Activity> index = mgr.getIndex(Activity.class, "/activity/byaccount/account1");
		index.addToIndex(act);
		index.addToIndex(act2);
		
		Activity act3 = new Activity();
		act3.setName("hello");
		act3.setNumTimes(6);
		mgr.put(act3);
		
		Index<Activity> index2 = mgr.getIndex(Activity.class, "/activity/byaccount/account2");
		index2.addToIndex(act3);
		
		//flush the persists and the index modifications to the database and index storage 
		mgr.flush();
		
		//Use the first index on the table and should find the two entities we added to that index..
		List<Activity> activities = Activity.findBetween(index, 3, 7);
		Assert.assertEquals(2, activities.size());
		
		//Use the second index on the table and should find the one entity on that
		//index yielding infinite scale if we partition indexes correctly
		List<Activity> activities2 = Activity.findBetween(index2, 3, 7);
		Assert.assertEquals(1, activities2.size());		
	}

}
