package com.alvazan.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.exc.ChildWithNoPkException;
import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;
import com.alvazan.test.db.EntityWithUUIDKey;
import com.alvazan.test.db.User;

public class TestManyToOne {

	private static final Logger log = LoggerFactory.getLogger(TestManyToOne.class);
	
	private static final String ACCOUNT_NAME = "dean";
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
	public void testActivityHasNullAccount() {
		//Activity has null reference to account
		Activity act = new Activity("act1");
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
		Activity activity = readWriteBasic(mgr);
		
		Activity reference = mgr.getReference(Activity.class, activity.getId());
		
		//Any calls to reference causes call to database(or cache) to fill the object in
		Assert.assertEquals(activity.getName(), reference.getName());
		Assert.assertEquals(activity.getNumTimes(), reference.getNumTimes());
	}
	
	@Test
	public void testWriteReadProxy() {
		Activity activity = readWriteBasic(mgr);
		
		//This is the proxy object
		Account account = activity.getAccount();
		//This will cause a load from the database
		Assert.assertEquals(ACCOUNT_NAME, account.getName());
	}
	
	@Test
	public void testWriteReadBasic() {
		readWriteBasic(mgr);
	}
	
	@Test
	public void testWriteActivityAccountNoSavedYet() {
		Account acc = new Account();
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		
		Activity act = new Activity("act1");
		act.setAccount(acc);
		act.setName("asdfsdf");
		act.setNumTimes(3);
		
		try {
			mgr.put(act);
			Assert.fail("Should have failed since account has no pk during activity save");
		} catch(ChildWithNoPkException e) {
			log.info("expected failure");
		}
	}

	@Test
	public void testNotfound() {
		Activity act = mgr.find(Activity.class, "asdf");
		Assert.assertNull(act);
	}
	
	@Test
	public void testFillInKeyMethod() {
		Account acc = new Account("acc1");
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		mgr.fillInWithKey(acc);
		
		Activity act = new Activity("act1");
		act.setAccount(acc);
		act.setName("asdfsdf");
		act.setNumTimes(3);
		
		mgr.put(act);
		
		mgr.flush();
		
		Activity activityResult = mgr.find(Activity.class, act.getId());
		Account theProxy = activityResult.getAccount();
		try {
			theProxy.getName();
			Assert.fail("Account was never saved so above line should fail");
		} catch(RowNotFoundException e) {
			log.info("this is expected");
		}
	}

	//@Test
	public void testToOneWithUUID() {
		Account acc1 = new Account();
		acc1.setId("acc1");
		acc1.setName("acc1name");
		mgr.fillInWithKey(acc1);

		EntityWithUUIDKey entity = new EntityWithUUIDKey();
		entity.setSomething("something");
		entity.setAccount(acc1);
		mgr.fillInWithKey(entity);
		// mgr.put(entity);

		User user = new User();
		user.setUuidEntity(entity);

		mgr.put(user);
		mgr.flush();

		User user2 = mgr.find(User.class, user.getId());
		Assert.assertNotNull(user2);
		Assert.assertEquals(entity.getId(), user2.getUuidEntity().getId());
	}

	private Activity readWriteBasic(NoSqlEntityManager mgr) {
		Account acc = new Account("acc1");
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		
		mgr.put(acc);
		
		Activity act = new Activity("act1");
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
