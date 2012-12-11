package com.alvazan.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.CursorToMany;
import com.alvazan.orm.api.base.CursorToManyImpl;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.impl.meta.data.collections.CursorProxy;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;
import com.alvazan.test.db.Email;
import com.alvazan.test.db.EmailAccountXref;
import com.alvazan.test.db.SomeEntity;
import com.alvazan.test.db.User;

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
		other.clearDatabase(true);
	}

	@Test
	public void testReturnsNullIfNotFound() {
		Activity act = mgr.find(Activity.class, "somekey");
		//not found returns null...
		Assert.assertNull(act);
		
		Activity act1 = new Activity("act1");
		act1.setName("dean");
		act1.setNumTimes(3);
		mgr.put(act1);
		
		mgr.flush();
		
		List<String> keyList = new ArrayList<String>();
		keyList.add(act1.getId());
		keyList.add("notexist");
		List<KeyValue<Activity>> results = mgr.findAllList(Activity.class, keyList);
		Assert.assertEquals(act1.getId(), results.get(0).getValue().getId());
		Assert.assertEquals(act1.getName(), results.get(0).getValue().getName());
		
		Assert.assertEquals("notexist", results.get(1).getKey());
		Assert.assertNull(results.get(1).getValue());
	}
	
	@Test
	public void testMultipleXrefs() {
		Account acc = new Account();
		mgr.put(acc);
		mgr.flush();
		acc = mgr.find(Account.class, acc.getId());
		
		User user = new User();
		user.setName("deab");
		User user2 = new User();
		user2.setName("bob");
		
		mgr.fillInWithKey(acc);
		mgr.fillInWithKey(user);
		mgr.fillInWithKey(user2);
		
		EmailAccountXref ref1 = new EmailAccountXref(user, acc);
		EmailAccountXref ref2 = new EmailAccountXref(user2, acc);
		
		mgr.put(ref1);
		mgr.put(ref2);
		mgr.put(acc);
		mgr.put(user);
		
		mgr.flush();
		
		Account result = mgr.find(Account.class, acc.getId());
		Assert.assertEquals(2, result.getEmails().size());
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
		addAndSaveActivity1(mgr, entity, "dean", "act1");
		addAndSaveActivity1(mgr, entity, "werwer", "act2");
		
		NoSqlEntityManager mgr1 = factory.createEntityManager();
		SomeEntity acc1 = mgr1.find(SomeEntity.class, entity.getId());
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		SomeEntity acc2 = mgr2.find(SomeEntity.class, entity.getId());
		
		deleteActivity(mgr1, acc1, "dean");
		deleteActivity(mgr2, acc2, "werwer");

		NoSqlEntityManager mgr3 = factory.createEntityManager();
		//Now, we should have no activities in our account list
		SomeEntity theAccount = mgr3.find(SomeEntity.class, entity.getId());
		Assert.assertEquals(0, theAccount.getActivities().size());
	}
	
	@Test
	public void testCachingOut() {
		SomeEntity entity = new SomeEntity();
		entity.setName("asdf");
		mgr.put(entity);
		mgr.flush();
		
		SomeEntity newEntity = mgr.find(SomeEntity.class, entity.getId());
		Assert.assertEquals(0, newEntity.getActivities().size());
		
		addAndSaveActivity1(mgr, entity, "dean", "act1");
		
		SomeEntity second = mgr.find(SomeEntity.class, entity.getId());
		Assert.assertEquals(1,  second.getActivities().size());
		
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
		
		addAndSaveActivity1(mgr1, acc1, "dean", "act1");
		addAndSaveActivity1(mgr2, acc2, "xxxx", "act2");

		NoSqlEntityManager mgr3 = factory.createEntityManager();
		//Now, we should have no activities in our account list
		SomeEntity theAccount = mgr3.find(SomeEntity.class, entity.getId());
		Assert.assertEquals(2, theAccount.getActivities().size());
	}

	private void addAndSaveActivity1(NoSqlEntityManager mgr1, SomeEntity acc1,
			String name, String actId) {
		Activity act = new Activity(actId);
		act.setName(name);
		act.setNumTimes(5);
		mgr1.put(act);
		
		acc1.putActivity(act);
		mgr1.put(acc1);
		
		mgr1.flush();
	}

	@Test
	public void testOneToManyWithMap() {
		Activity act1 = new Activity("act1");
		act1.setName("dean");
		act1.setNumTimes(3);
		mgr.put(act1);
		Activity act2 = new Activity("act2");
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
	public void testIndependentAddsAreCumulativeForCursor() {
		Account acc = new Account("acc1");
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		
		mgr.put(acc);
		mgr.flush();

		NoSqlEntityManager mgr1 = factory.createEntityManager();
		Account acc1 = mgr1.find(Account.class, acc.getId());
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		Account acc2 = mgr2.find(Account.class, acc.getId());
		
		addAndSaveActivity1(mgr1, acc1, "dean", "act1");
		addAndSaveActivity1(mgr2, acc2, "xxxx", "act2");

		NoSqlEntityManager mgr3 = factory.createEntityManager();
		//Now, we should have no activities in our account list
		Account theAccount = mgr3.find(Account.class, acc.getId());
		
		CursorToMany<Activity> cursor = theAccount.getActivitiesCursor();
		int counter = 0;
		while(cursor.next()) {
			Activity current = cursor.getCurrent();
			if(counter == 0)
				Assert.assertEquals("dean", current.getName());
			counter++;
		}
		
		Assert.assertEquals(2, counter);
	}
	
	@Test
	public void testClearAfterAddForCursor() {
		Account acc = new Account("acc1");
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		
		mgr.put(acc);
		mgr.flush();

		NoSqlEntityManager mgr = factory.createEntityManager();
		Account acc1 = mgr.find(Account.class, acc.getId());
		
		addAndSaveActivity1(mgr, acc1, "dean", "act1");
		Assert.assertEquals(0, ((CursorProxy<Activity>)acc1.getActivitiesCursor()).getElementsToAdd().size());
		addAndSaveActivity1(mgr, acc1, "xxxx", "act2");
		Assert.assertEquals(0, ((CursorProxy<Activity>)acc1.getActivitiesCursor()).getElementsToAdd().size());
		addAndSaveActivity1(mgr, acc1, "yyyy", "act3");
		Assert.assertEquals(0, ((CursorProxy<Activity>)acc1.getActivitiesCursor()).getElementsToAdd().size());

		NoSqlEntityManager mgr3 = factory.createEntityManager();
		//Now, we should have no activities in our account list
		Account theAccount = mgr3.find(Account.class, acc.getId());
		
		CursorToMany<Activity> cursor = theAccount.getActivitiesCursor();
		int counter = 0;
		while(cursor.next()) {
			Activity current = cursor.getCurrent();
			if(counter == 0)
				Assert.assertEquals("dean", current.getName());
			counter++;
		}
		
		Assert.assertEquals(3, counter);
	}
	
	@Test
	public void testClearAfterAddForNewCursor() {
		Account acc1 = new Account("acc1");
		acc1.setName(ACCOUNT_NAME);
		acc1.setUsers(5.0f);
		
		mgr.put(acc1);
		mgr.flush();

		NoSqlEntityManager mgr = factory.createEntityManager();
		
		addAndSaveActivity1(mgr, acc1, "dean", "act1");
		Assert.assertEquals(0, ((CursorToManyImpl<Activity>)acc1.getActivitiesCursor()).getElementsToAdd().size());
		addAndSaveActivity1(mgr, acc1, "xxxx", "act2");
		Assert.assertEquals(0, ((CursorToManyImpl<Activity>)acc1.getActivitiesCursor()).getElementsToAdd().size());
		addAndSaveActivity1(mgr, acc1, "yyyy", "act3");
		Assert.assertEquals(0, ((CursorToManyImpl<Activity>)acc1.getActivitiesCursor()).getElementsToAdd().size());

		NoSqlEntityManager mgr3 = factory.createEntityManager();
		//Now, we should have no activities in our account list
		Account theAccount = mgr3.find(Account.class, acc1.getId());
		
		CursorToMany<Activity> cursor = theAccount.getActivitiesCursor();
		int counter = 0;
		while(cursor.next()) {
			Activity current = cursor.getCurrent();
			if(counter == 0)
				Assert.assertEquals("dean", current.getName());
			counter++;
		}
		
		Assert.assertEquals(3, counter);
	}
	
	@Test
	public void testIndependentAddsAreCumulative() {
		Account acc = new Account("acc1");
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		
		mgr.put(acc);
		mgr.flush();

		NoSqlEntityManager mgr1 = factory.createEntityManager();
		Account acc1 = mgr1.find(Account.class, acc.getId());
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		Account acc2 = mgr2.find(Account.class, acc.getId());
		
		addAndSaveActivity1(mgr1, acc1, "dean", "act1");
		addAndSaveActivity1(mgr2, acc2, "xxxx", "act2");

		NoSqlEntityManager mgr3 = factory.createEntityManager();
		//Now, we should have no activities in our account list
		Account theAccount = mgr3.find(Account.class, acc.getId());
		Assert.assertEquals(2, theAccount.getActivities().size());
	}

	private void addAndSaveActivity1(NoSqlEntityManager mgr1, Account acc1, String name, String actId) {
		Activity act = new Activity(actId);
		act.setName(name);
		act.setIsCool(true);
		mgr1.put(act);
		
		acc1.addActivity(act);
		mgr1.put(acc1);
		
		mgr1.flush();
	}

	@Test
	public void testContainsMethod() {
		Account acc = new Account("acc1");
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		mgr.fillInWithKey(acc);
		Activity act1 = new Activity("act1");
		act1.setAccount(acc);
		act1.setName("dean");
		act1.setNumTimes(3);
		
		mgr.put(act1);
		
		Activity act2 = new Activity("act2");
		act2.setName("dean");
		act2.setNumTimes(4);
		
		mgr.put(act2);
		
		acc.addActivity(act1);
		acc.addActivity(act2);
		mgr.put(acc);
		
		mgr.flush();
		
		Account find = mgr.find(Account.class, acc.getId());
		Activity act = mgr.find(Activity.class, act1.getId());
		
		Assert.assertFalse(find.getActivities().contains(act.getId()));
		Assert.assertTrue(find.getActivities().contains(act));
	}
	
	@Test
	public void testRemovesAreCumulative() {
		Account acc = new Account("acc1");
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		mgr.fillInWithKey(acc);
		Activity act1 = new Activity("act1");
		act1.setAccount(acc);
		act1.setName("dean");
		act1.setNumTimes(3);
		
		mgr.put(act1);
		
		Activity act2 = new Activity("act2");
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

		NoSqlEntityManager mgr3 = factory.createEntityManager();
		//Now, we should have no activities in our account list
		Account theAccount = mgr3.find(Account.class, acc.getId());
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
		Account acc = new Account("acc1");
		acc.setName(ACCOUNT_NAME);
		acc.setUsers(5.0f);
		mgr.fillInWithKey(acc);
		Activity act1 = new Activity("act1");
		act1.setAccount(acc);
		act1.setName("dean");
		act1.setNumTimes(3);
		
		mgr.put(act1);
		
		Activity act2 = new Activity("act2");
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

	//@Test
	public void testEmbedded() {
		Email sub = new Email();
		sub.setId("sub");
		sub.setName("dean");
		
		Email e1 = new Email();
		e1.setId("e1");
		e1.setName("qwer");
		e1.getEmails().add(sub);
		
		Email e2 = new Email();
		e2.setId("e2");
		e2.setName("asdf");

		User user = new User();
		user.getEmails().add(e1);
		user.getEmails().add(e2);
		
		mgr.put(user);
		mgr.flush();
		
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		User user2 = mgr2.find(User.class, user.getId());
		
		List<Email> emails = user2.getEmails();
		Email email = emails.get(0);
		Assert.assertNotNull(email);
		Assert.assertEquals(e1.getName(), email.getName());
		
		Email subResult = email.getEmails().get(0);
		Assert.assertEquals(sub.getName(), subResult.getName());
		
		Email email2 = emails.get(1);
		Assert.assertEquals(e2.getName(), email2.getName());
	}
	
	@Test
	public void testEmbeddedSimple() {
		Email sub = new Email();
		sub.setId("sub");
		sub.setName("dean");
		
		sub.getIds().add("one");
		sub.getIds().add("two");
		
		sub.getInts().add(5);
		sub.getInts().add(8);
		
		mgr.put(sub);
		mgr.flush();
		
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		Email email = mgr2.find(Email.class, sub.getId());
		NoSqlEntityManager mgr3 = factory.createEntityManager();
		Email email2 = mgr3.find(Email.class, sub.getId());
		
		List<String> ids = email.getIds();
		Assert.assertEquals("one", ids.get(0));
		
		List<Integer> nums = email.getInts();
		Assert.assertEquals(new Integer(5), nums.get(0));
		
		email.getInts().remove(0);
		email.getIds().remove("one");
		mgr2.put(email);
		mgr2.flush();
		
		email2.getInts().add(12);
		email2.getIds().add("zzzz");
		mgr3.put(email2);
		mgr3.flush();
		
		NoSqlEntityManager mgr4 = factory.createEntityManager();
		Email emailF = mgr4.find(Email.class, sub.getId());
		
		Assert.assertEquals(2, emailF.getInts().size());
		Assert.assertEquals(2, emailF.getIds().size());

		Assert.assertEquals(new Integer(8), emailF.getInts().get(0));
		Assert.assertEquals(new Integer(12), emailF.getInts().get(1));
		
		Assert.assertEquals("two", emailF.getIds().get(0));
		Assert.assertEquals("zzzz", emailF.getIds().get(1));
	}

}
