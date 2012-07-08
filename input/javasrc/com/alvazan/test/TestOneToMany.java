package com.alvazan.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.AbstractBootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;

public class TestOneToMany {

	private static final Logger log = LoggerFactory.getLogger(TestOneToMany.class);
	
	private static final String ACCOUNT_NAME = "declan";
	private NoSqlEntityManagerFactory factory;

	@Before
	public void setup() {
		factory = AbstractBootstrap.create(DbTypeEnum.IN_MEMORY);
		factory.setup(null, "com.alvazan.test.db");
	}
	@Test
	public void testInListOneValueIsNotInDatabase() {
	}
	
	public void testBasicToMany() {
		NoSqlEntityManager mgr = factory.createEntityManager();
		readWriteBasic(mgr);
	}

	private Account readWriteBasic(NoSqlEntityManager mgr) {
		Account acc = new Account();
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		mgr.fillInWithKey(acc);
		
		Activity act1 = new Activity();
		act1.setAccount(acc);
		act1.setName("asdfsdf");
		act1.setNumTimes(3);
		
		mgr.put(act1);

		Activity act2 = new Activity();
		act2.setName("dean");
		act2.setNumTimes(4);

		mgr.put(act2);
		
		acc.addActivity(act1);
		acc.addActivity(act2);
		mgr.put(acc);
		
		mgr.flush();
		
		Account accountResult = mgr.find(Account.class, acc.getId());
		Assert.assertEquals(ACCOUNT_NAME, accountResult.getName());
		Assert.assertEquals(acc.getUsers(), accountResult.getUsers());
		Assert.assertEquals(2, acc.getActivities().size());
		
		return accountResult;
	}
}
