package com.alvazan.test;

import java.math.BigInteger;
import java.util.List;

import junit.framework.Assert;

import org.joda.time.LocalDateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Activity;
import com.eaio.uuid.UUID;

public class TestIndexTypes {

	private static final Logger log = LoggerFactory.getLogger(TestIndexTypes.class);
	private static NoSqlEntityManagerFactory factory;
	private NoSqlEntityManager mgr;
	private LocalDateTime time;
	private UUID uid;

	@BeforeClass
	public static void setup() {
		factory = FactorySingleton.createFactoryOnce();
	}
	
	@Before
	public void createEntityManager() {
		mgr = factory.createEntityManager();
		setupRecords();
	}
	@After
	public void clearDatabase() {
		NoSqlEntityManager other = factory.createEntityManager();
		other.clearDatabase(true);
	}
	
	@Test
	public void testBasicString() {
		List<Activity> findByName = Activity.findByName(mgr, "hello");
		Assert.assertEquals(1, findByName.size());
		
		List<Activity> zero = Activity.findByName(mgr, "asdf");
		Assert.assertEquals(0, zero.size());
	}

	@Test
	public void testBasicBoolean() {
		List<Activity> list = Activity.findByCool(mgr, true);
		Assert.assertEquals(1, list.size());
		
		List<Activity> zero = Activity.findByCool(mgr, false);
		Assert.assertEquals(0, zero.size());
	}
	
	@Test
	public void testBasicLong() {
		List<Activity> list = Activity.findNumTimes(mgr, 5L);
		Assert.assertEquals(1, list.size());
		
		List<Activity> zero = Activity.findNumTimes(mgr, 0L);
		Assert.assertEquals(0, zero.size());
	}

	@Test
	public void testBasicLocalTime() {
		List<Activity> list = Activity.findByLocalDateTime(mgr, time);
		Assert.assertEquals(1, list.size());
		
		LocalDateTime t = time.plusMillis(1);
		
		List<Activity> zero = Activity.findByLocalDateTime(mgr, t);
		Assert.assertEquals(0, zero.size());
	}
	
	@Test
	public void testBasicFloat() {
		List<Activity> list = Activity.findByFloat(mgr, 5.65f);
		Assert.assertEquals(1, list.size());
		
		List<Activity> zero = Activity.findByFloat(mgr, 5.66f);
		Assert.assertEquals(0, zero.size());		
	}

    @Test
    public void testBigInteger() {
        BigInteger bigInt = BigInteger.valueOf(Long.MAX_VALUE+87);
        List<Activity> list = Activity.findByBigInt(mgr, bigInt);
        Assert.assertEquals(1, list.size());

        BigInteger bigInt1 = BigInteger.valueOf(Long.MAX_VALUE+89);
        List<Activity> zero = Activity.findByBigInt(mgr, bigInt1);
        Assert.assertEquals(0, zero.size());
    }

	private void setupRecords() {
		Activity act = new Activity("act1");
		act.setName("hello");
		act.setMyFloat(5.65f);
		act.setUniqueColumn("notunique");
		act.setNumTimes(5);
		act.setIsCool(true);
		BigInteger bigInt = BigInteger.valueOf(Long.MAX_VALUE+87);
		act.setBigInt(bigInt);
		time = LocalDateTime.now();
		act.setDate(time);
		uid = new UUID();
		act.setUniqueId(uid);
		mgr.put(act);
		
		//Everything is null for this activity so queries above should not find him...
		Activity act2 = new Activity("act2");
		act2.setNumTimes(58);
		mgr.put(act2);
		
		mgr.flush();
	}

}
