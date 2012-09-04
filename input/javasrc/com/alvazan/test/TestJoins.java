package com.alvazan.test;

import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.AAPartitionedTrade;
import com.alvazan.test.db.PartSecurity;

public class TestJoins {

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
	
	//@Test
	//public void testEmpty() {}
	
	@Test
	public void testJoin() {
		putEntities();
		
		List<AAPartitionedTrade> trades = AAPartitionedTrade.findInPartition(mgr, 5, "one", null);
		Assert.assertEquals(2, trades.size());
	}
	
	private void putEntities() {
		PartSecurity sec = new PartSecurity();
		sec.setSecurityType("one");
		mgr.put(sec);
		
		PartSecurity sec2 = new PartSecurity();
		sec2.setSecurityType("two");
		mgr.put(sec2);
		
		PartSecurity sec3 = new PartSecurity();
		sec3.setSecurityType("one");
		mgr.put(sec3);
		
		mgr.flush();

		//This trade has no account so is in the null partition of accounts
		AAPartitionedTrade trade1 = new AAPartitionedTrade();
		trade1.setSecurity(sec);
		trade1.setNumShares(5);
		mgr.put(trade1);
		
		AAPartitionedTrade trade2 = new AAPartitionedTrade();
		trade2.setSecurity(sec);
		trade2.setNumShares(6);
		mgr.put(trade2);

		AAPartitionedTrade trade3 = new AAPartitionedTrade();
		trade3.setSecurity(sec3);
		trade3.setNumShares(5);
		mgr.put(trade3);

		//has null security
		AAPartitionedTrade trade4 = new AAPartitionedTrade();
		trade4.setNumShares(5);
		mgr.put(trade4);
		
		//has security 2 not one
		AAPartitionedTrade trade5 = new AAPartitionedTrade();
		trade5.setSecurity(sec2);
		trade5.setNumShares(5);
		mgr.put(trade5);
		
		mgr.flush();
		
	}
}
