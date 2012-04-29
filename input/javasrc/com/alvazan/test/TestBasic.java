package com.alvazan.test;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

import com.alvazan.orm.api.Bootstrap;
import com.alvazan.orm.api.DbTypeEnum;
import com.alvazan.orm.api.NoSqlEntityManager;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;

public class TestBasic {

	private static final String ACCOUNT_NAME = "dean";
	private NoSqlEntityManagerFactory factory;

	@Before
	public void setup() {
		factory = Bootstrap.create(DbTypeEnum.IN_MEMORY);
		factory.setup(null, "com.alvazan.test.db");
	}
	@Test
	public void testActivityHasNullAccount() {
		NoSqlEntityManager mgr = factory.createEntityManager();

		//Activity has null reference to account
		Activity act = new Activity();
		act.setName("asdfsdf");
		act.setNumTimes(3);
		
		mgr.put(act);
		mgr.flush();
		
		Activity activityResult = mgr.find(Activity.class, act.getId());
		Assert.assertEquals(act.getName(), activityResult.getName());
		Assert.assertEquals(act.getNumTimes(), activityResult.getNumTimes());		
	}
	
	@Test
	public void testGetReference() {
		NoSqlEntityManager mgr = factory.createEntityManager();
		
		Activity activity = readWriteBasic(mgr);
		
		Activity reference = mgr.getReference(Activity.class, activity.getId());
		
		//Any calls to reference causes call to database(or cache) to fill the object in
		Assert.assertEquals(activity.getName(), reference.getName());
		Assert.assertEquals(activity.getNumTimes(), reference.getNumTimes());
	}
	
	@Test
	public void testWriteReadProxy() {
		NoSqlEntityManager mgr = factory.createEntityManager();
		
		Activity activity = readWriteBasic(mgr);
		
		//This is the proxy object
		Account account = activity.getAccount();
		//This will cause a load from the database
		Assert.assertEquals(ACCOUNT_NAME, account.getName());
	}
	
	@Test
	public void testWriteReadBasic() {
		NoSqlEntityManager mgr = factory.createEntityManager();
		
		readWriteBasic(mgr);
	}
	
	private Activity readWriteBasic(NoSqlEntityManager mgr) {
		Account acc = new Account();
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5);
		
		mgr.put(acc);
		
		Activity act = new Activity();
		act.setAccount(acc);
		act.setName("asdfsdf");
		act.setNumTimes(3);
		
		mgr.put(act);
		
		mgr.flush();
		
		Account accountResult = mgr.find(Account.class, acc.getId());
		Assert.assertEquals(ACCOUNT_NAME, accountResult.getName());
		Assert.assertEquals(acc.getUsers(), accountResult.getUsers());
		
		Activity activityResult = mgr.find(Activity.class, act.getId());
		Assert.assertEquals(act.getName(), activityResult.getName());
		Assert.assertEquals(act.getNumTimes(), activityResult.getNumTimes());
		Assert.assertEquals(acc.getId(), activityResult.getAccount().getId());
		
		return activityResult;
	}
}
