package com.alvazan.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Email;
import com.alvazan.test.db.EmbeddedEmail;
import com.alvazan.test.db.EmbeddedEntityWithNoId;
import com.alvazan.test.db.User;

public class TestEmbedded {

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
	public void testEmbedded() {
		EmbeddedEmail sub = new EmbeddedEmail();
		sub.setIdkey(67);
		sub.setName("dean");
		sub.setType("nosqltype");
		
		EmbeddedEmail e1 = new EmbeddedEmail();
		e1.setIdkey(65);
		e1.setName("name1");
		e1.setType("type1");
		//e1.getEmails().add(sub);
		
		EmbeddedEmail e2 = new EmbeddedEmail();
		e2.setIdkey(66);
		e2.setName("name2");
		e2.setType("type2");
		
		User user = new User();
		List<EmbeddedEmail> listEmails = new ArrayList<EmbeddedEmail>();
		listEmails.add(e1);
		listEmails.add(e2);
		user.setEmails(listEmails);
		user.setEmail(sub);

		mgr.fillInWithKey(e1);
		mgr.fillInWithKey(e1);
		mgr.fillInWithKey(sub);
		
		mgr.put(user);
		mgr.flush();
		
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		User user2 = mgr2.find(User.class, user.getId());
		
		//Check single entity
		EmbeddedEmail emailSub = user2.getEmail();
		Assert.assertNotNull(emailSub);
		Assert.assertEquals(sub.getIdkey(), emailSub.getIdkey());
		Assert.assertEquals(sub.getName(), emailSub.getName());
		
		//Check List of entities
		List<EmbeddedEmail> emails = user2.getEmails();

		EmbeddedEmail email = emails.get(0);
		Assert.assertNotNull(email);
		
		Assert.assertEquals(e1.getIdkey(), email.getIdkey());
		Assert.assertEquals(e1.getName(), email.getName());
		
		EmbeddedEmail email2 = emails.get(1);
		Assert.assertEquals(e2.getName(), email2.getName());

		// To check if delete is working fine
		mgr.remove(user);
		mgr.flush();
	}

	@Test
	public void testEmbeddedWithoutId() {
		// Now check if an Embedded Entity without NoSqlId works or not
		EmbeddedEntityWithNoId embedWOId = new EmbeddedEntityWithNoId();
		embedWOId.setId("someid");
		embedWOId.setName("someName");
		embedWOId.setType("someType");

		User user = new User();
		user.setEntityWOId(embedWOId);

		mgr.put(user);
		mgr.flush();

		NoSqlEntityManager mgr2 = factory.createEntityManager();
		User user2 = mgr2.find(User.class, user.getId());

		EmbeddedEntityWithNoId embedWOId2 = user2.getEntityWOId();
		Assert.assertNotNull(embedWOId2);
		Assert.assertEquals(embedWOId.getId(), embedWOId2.getId());
		Assert.assertEquals(embedWOId.getName(), embedWOId2.getName());
		Assert.assertEquals(embedWOId.getType(), embedWOId2.getType());
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
	
	@Test
	public void testEmbeddedMap() {
		Email sub = new Email();
		sub.setId("sub");
		sub.setName("dean");
		Map<String, Integer> keyToVal = sub.getKeyToVal();
		keyToVal.put("someVal", 5);
		keyToVal.put("other", 6);
		
		mgr.put(sub);
		mgr.flush();
		
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		Email email = mgr2.find(Email.class, sub.getId());

		Map<String, Integer> nextMap = email.getKeyToVal();
		Assert.assertEquals(2, nextMap.size());
		Assert.assertEquals(new Integer(5), nextMap.get("someVal"));

		nextMap.remove("someVal");
		
		mgr2.put(email);
		mgr2.flush();
		
		NoSqlEntityManager mgr3 = factory.createEntityManager();
		Email email3 = mgr3.find(Email.class, sub.getId());
		
		Map<String, Integer> finalMap = email3.getKeyToVal();
		Assert.assertEquals(1, finalMap.size());
	}
	
	@Test
	public void testEmbeddedMap2() {
		Email sub = new Email();
		sub.setId("sub");
		sub.setName("dean");
		Map<Integer, String> keyToVal = sub.getSomeMap();
		keyToVal.put(5, "someVal");
		keyToVal.put(6, "other");

		mgr.put(sub);
		mgr.flush();
		
		NoSqlEntityManager mgr2 = factory.createEntityManager();
		Email email = mgr2.find(Email.class, sub.getId());

		Map<Integer, String> nextMap = email.getSomeMap();
		Assert.assertEquals(2, nextMap.size());
		Assert.assertEquals("someVal", nextMap.get(5));

		nextMap.remove(6);
		
		mgr2.put(email);
		mgr2.flush();
		
		NoSqlEntityManager mgr3 = factory.createEntityManager();
		Email email3 = mgr3.find(Email.class, sub.getId());

		Map<Integer, String> finalMap = email3.getSomeMap();
		Assert.assertEquals(1, finalMap.size());
	}
}
