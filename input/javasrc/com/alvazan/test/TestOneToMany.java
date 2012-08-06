package com.alvazan.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.KeyValue;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;
import com.alvazan.test.db.SomeEntity;

public class TestOneToMany {

	private static final String ACCOUNT_NAME = "declan";
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
		other.clearDbAndIndexesIfInMemoryType();
	}

	@Test
	public void testReturnsNullIfNotFound() {
		Activity act = mgr.find(Activity.class, "somekey");
		//not found returns null...
		Assert.assertNull(act);
		
		Activity act1 = new Activity();
		act1.setName("dean");
		act1.setNumTimes(3);
		mgr.put(act1);
		
		mgr.flush();
		
		List<String> keyList = new ArrayList<String>();
		keyList.add(act1.getId());
		keyList.add("notexist");
		List<KeyValue<Activity>> results = mgr.findAll(Activity.class, keyList);
		Assert.assertEquals(act1.getName(), results.get(0).getValue().getName());
		
		Assert.assertEquals("notexist", results.get(1).getKey());
		Assert.assertNull(results.get(1).getValue());
	}
	
	@Test
	public void testEmptyMap() {
		SomeEntity entity = new SomeEntity();
		entity.setName("asdf");
		mgr.put(entity);
		mgr.flush();
		
		SomeEntity someEntity = mgr.find(SomeEntity.class, entity.getId());
		Assert.assertEquals(0, someEntity.getActivities().size());
	}
	
	@Test
	public void testMapDeletesAreCumulative() {
		SomeEntity entity = new SomeEntity();
		entity.setName("asdf");
		mgr.put(entity);
		mgr.flush();
		addAndSaveActivity1(mgr, entity, "dean");
		addAndSaveActivity1(mgr, entity, "werwer");
		
		NoSqlEntityManager mgr1 = factory.createEntityManager();
		SomeEntity acc1 = mgr1.find(SomeEntity.class, entity.getId());
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		SomeEntity acc2 = mgr2.find(SomeEntity.class, entity.getId());
		
		deleteActivity(mgr1, acc1, "dean");
		deleteActivity(mgr2, acc2, "werwer");

		//Now, we should have no activities in our account list
		SomeEntity theAccount = mgr.find(SomeEntity.class, entity.getId());
		Assert.assertEquals(0, theAccount.getActivities().size());
	}
	
	private void deleteActivity(NoSqlEntityManager mgr1, SomeEntity acc1, String name) {
		acc1.remove(name);
		mgr1.put(acc1);
		mgr1.flush();
	}

	@Test
	public void testMapAddsAreCumulative() {
		SomeEntity entity = new SomeEntity();
		entity.setName("asdf");
		mgr.put(entity);
		
		mgr.flush();
		
		NoSqlEntityManager mgr1 = factory.createEntityManager();
		SomeEntity acc1 = mgr1.find(SomeEntity.class, entity.getId());
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		SomeEntity acc2 = mgr2.find(SomeEntity.class, entity.getId());
		
		addAndSaveActivity1(mgr1, acc1, "dean");
		addAndSaveActivity1(mgr2, acc2, "xxxx");

		//Now, we should have no activities in our account list
		SomeEntity theAccount = mgr.find(SomeEntity.class, entity.getId());
		Assert.assertEquals(2, theAccount.getActivities().size());
	}

	private void addAndSaveActivity1(NoSqlEntityManager mgr1, SomeEntity acc1,
			String name) {
		Activity act = new Activity();
		act.setName(name);
		act.setNumTimes(5);
		mgr1.put(act);
		
		acc1.putActivity(act);
		mgr1.put(acc1);
		
		mgr1.flush();
	}

	@Test
	public void testOneToManyWithMap() {
		Activity act1 = new Activity();
		act1.setName("dean");
		act1.setNumTimes(3);
		mgr.put(act1);
		Activity act2 = new Activity();
		act2.setName("dean2");
		act2.setNumTimes(4);
		mgr.put(act2);
		
		SomeEntity entity = new SomeEntity();
		entity.setName("asdf");
		entity.putActivity(act1);
		entity.putActivity(act2);
		mgr.put(entity);
		
		mgr.flush();
		
		SomeEntity result = mgr.find(SomeEntity.class, entity.getId());
		
		Activity resAct1 = result.getActivity(act1.getName());
		Assert.assertEquals(act1.getNumTimes(), resAct1.getNumTimes());
		
		Activity resAct2 = result.getActivity(act2.getName());
		Assert.assertEquals(act2.getNumTimes(), resAct2.getNumTimes());
	}

	@Test
	public void testIndependentAddsAreCumulative() {
		Account acc = new Account();
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		
		mgr.put(acc);
		mgr.flush();

		NoSqlEntityManager mgr1 = factory.createEntityManager();
		Account acc1 = mgr1.find(Account.class, acc.getId());
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		Account acc2 = mgr2.find(Account.class, acc.getId());
		
		addAndSaveActivity1(mgr1, acc1, "dean");
		addAndSaveActivity1(mgr2, acc2, "xxxx");

		//Now, we should have no activities in our account list
		Account theAccount = mgr.find(Account.class, acc.getId());
		Assert.assertEquals(2, theAccount.getActivities().size());
	}
	
	private void addAndSaveActivity1(NoSqlEntityManager mgr1, Account acc1, String name) {
		Activity act = new Activity();
		act.setName(name);
		act.setIsCool(true);
		mgr1.put(act);
		
		acc1.addActivity(act);
		mgr1.put(acc1);
		
		mgr1.flush();
	}

	@Test
	public void testRemovesAreCumulative() {
		Account acc = new Account();
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		mgr.fillInWithKey(acc);
		Activity act1 = new Activity();
		act1.setAccount(acc);
		act1.setName("dean");
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

		NoSqlEntityManager mgr1 = factory.createEntityManager();
		Account acc1 = mgr1.find(Account.class, acc.getId());
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		Account acc2 = mgr2.find(Account.class, acc.getId());
		
		removeActivity1(mgr1, acc1, act1);
		removeActivity1(mgr2, acc2, act2);

		//Now, we should have no activities in our account list
		Account theAccount = mgr.find(Account.class, acc.getId());
		Assert.assertEquals(0, theAccount.getActivities().size());
	}
	
	private void removeActivity1(NoSqlEntityManager mgr, Account acc, Activity act) {
		List<Activity> activities = acc.getActivities();
		for(int i = 0; i < activities.size(); i++) {
			Activity activity = activities.get(i);
			if(activity.getId().equals(act.getId())) {
				activities.remove(i);
				break;
			}
		}
		
		mgr.put(acc);
		mgr.flush();
	}

	@Test
	public void testOneToManyWithList() {
		Account acc = new Account();
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		mgr.fillInWithKey(acc);
		Activity act1 = new Activity();
		act1.setAccount(acc);
		act1.setName("dean");
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
		List<Activity> activities = accountResult.getActivities();
		Assert.assertEquals(2, activities.size());
		
		//Now let's force proxy creation by getting one of the Activities
		Activity activity = activities.get(0);
		
		//This should NOT hit the database since the id is wrapped by the proxy and exists already
		String id = activity.getId();
		//since we added activity1 first, we better see that same activity be first in the list again...
		Assert.assertEquals(act1.getId(), id);
		
		//Now let's force a database lookup to have the activity filled in
		Assert.assertEquals("dean", activity.getName());
	}

}
