package com.alvazan.test;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.test.db.Activity;
import com.alvazan.test.db.EmailAccountXref;
import com.alvazan.test.db.InheritanceSub1;
import com.alvazan.test.db.InheritanceSub2;
import com.alvazan.test.db.NonVirtSub1;
import com.alvazan.test.db.NonVirtSub2;
import com.alvazan.test.db.NonVirtSuper;
import com.alvazan.test.db.TimeSeriesData;
import com.alvazan.test.db.User;

public class TestIndexesNew {

	private static final Logger log = LoggerFactory.getLogger(TestIndexesNew.class);
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
		try {
			other.clearDatabase(true);
		} catch(Exception e) {
			if (log.isWarnEnabled())
				log.warn("Could not clean up properly", e);
		}
	}
	
	@Test
	public void testAllRowsVirtual() {
        if (FactorySingleton.getServerType() != DbTypeEnum.CASSANDRA)
            return;

		InheritanceSub1 sub1 = new InheritanceSub1();
		sub1.setName("dean");
		InheritanceSub2 sub2 = new InheritanceSub2();
		sub2.setNum(5);
		Activity act = new Activity();
		act.setId("myid");
		act.setName("joe");
		mgr.put(sub1);
		mgr.put(sub2);
		mgr.put(act);
		mgr.flush();
		
		Cursor<Object> cursor = mgr.allRows(Object.class, "ourstuff", 500);
		int count = 0;
		while(cursor.next()) {
			Object current = cursor.getCurrent();
			count++;
		}
		Assert.assertEquals(3, count);
	}

	@Test
	public void testAllRowsPolymorphic() {
		if (FactorySingleton.getServerType() != DbTypeEnum.CASSANDRA && FactorySingleton.getServerType() != DbTypeEnum.IN_MEMORY)
			return;

		NonVirtSub1 s1 = new NonVirtSub1();
		s1.setName("dean");
		NonVirtSub2 s2 = new NonVirtSub2();
		s2.setNum(5);
	
		mgr.put(s1);
		mgr.put(s2);
		
		mgr.flush();

		Cursor<NonVirtSuper> cursor = mgr.allRows(NonVirtSuper.class, "NonVirtSuper", 500);
		int count = 0;
		while(cursor.next()) {
			NonVirtSuper current = cursor.getCurrent();
			count++;
		}
		Assert.assertEquals(2, count);
	}
	
	@Test
	public void testInheritanceWithCassandraFindAll() {

		NonVirtSub1 s1 = new NonVirtSub1();
		s1.setName("dean");
		NonVirtSub2 s2 = new NonVirtSub2();
		s2.setNum(5);
	
		mgr.put(s1);
		mgr.put(s2);
		
		mgr.flush();
		
		Cursor<KeyValue<NonVirtSub1>> cursor = NonVirtSub1.findAll(mgr);
		while(cursor.next()) {
			KeyValue<NonVirtSub1> kv = cursor.getCurrent();
			NonVirtSub1 sub = kv.getValue();
			Assert.assertEquals(s1.getName(), s1.getName());
		}
	}
	
	@Test
	public void testNoPlayOrmIndexButUseCassandraFindAll() {

		EmailAccountXref ref = new EmailAccountXref();
		EmailAccountXref ref2 = new EmailAccountXref();
		mgr.put(ref);
		mgr.put(ref2);
		mgr.flush();
		
		List<EmailAccountXref> accounts = EmailAccountXref.findAll(mgr);
		Assert.assertEquals(2, accounts.size());
	}
	
	@Test
	public void testBasicChangeToIndex() {
		User user = new User();
		user.setName("dean");
		user.setLastName("hiller");
		
		mgr.put(user);
		
		mgr.flush();
		
		User result = User.findByName(mgr, user.getName());
		Assert.assertEquals(user.getName(), result.getName());
		
		result.setName("dave");
		mgr.put(result);
		mgr.flush();
		
		User newResult = User.findByName(mgr, user.getName());
		Assert.assertNull(newResult);
		
		User newerResult = User.findByName(mgr, result.getName());
		Assert.assertEquals(result.getName(), newerResult.getName());
		
	}
	
	@Test
	public void testBasicRemove() {
		User user = new User();
		user.setName("dean");
		user.setLastName("hiller");
		
		mgr.put(user);
		
		mgr.flush();
		
		User result = User.findByName(mgr, user.getName());
		Assert.assertEquals(user.getName(), result.getName());
		
		mgr.remove(user);
		
		mgr.flush();
		
		User newResult = User.findByName(mgr, user.getName());
		Assert.assertNull(newResult);
	}
	
	@Test
	public void testPrimaryKeyLongIndex() {
		TimeSeriesData data = new TimeSeriesData();
		data.setKey(67L);
		data.setSomeName("dean");
		
		mgr.put(data);
		mgr.flush();

		TimeSeriesData newData = TimeSeriesData.findById(mgr, data.getKey());
		Assert.assertEquals(data.getSomeName(), newData.getSomeName());
	}
	
	@Test
	public void testFloatKey() {
		TimeSeriesData data = new TimeSeriesData();
		data.setKey(67L);
		data.setSomeName("dean");
		data.setTemp(67.8f);
		
		mgr.put(data);
		mgr.flush();

		TimeSeriesData newData = TimeSeriesData.findByTemp(mgr, 67.8f);
		Assert.assertEquals(data.getSomeName(), newData.getSomeName());
	}
}
