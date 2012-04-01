package com.alvazan.test;

import junit.framework.Assert;

import com.alvazan.orm.api.Bootstrap;
import com.alvazan.orm.api.NoSqlEntityManager;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;

public class TestBasic {

	private NoSqlEntityManagerFactory factory;

	public void setup() {
		factory = Bootstrap.getSingleton();
	}
	
	public void testBasic() {
		NoSqlEntityManager mgr = factory.createEntityManager();
		
		Account acc = new Account();
		acc.setName("dean");
		acc.setUsers(5);
		
		mgr.put(acc);
		
		Activity act = new Activity();
		act.setAccount(acc);
		act.setName("asdfsdf");
		act.setNumTimes(3);
		
		mgr.put(act);
		
		mgr.flush();
		
		Account accountResult = mgr.find(Account.class, acc.getId());
		Assert.assertEquals(acc.getName(), accountResult.getName());
		Assert.assertEquals(acc.getUsers(), accountResult.getUsers());
		
		Activity activityResult = mgr.find(Activity.class, act.getId());
		Assert.assertEquals(act.getName(), activityResult.getName());
		Assert.assertEquals(act.getNumTimes(), activityResult.getNumTimes());
	}
}
