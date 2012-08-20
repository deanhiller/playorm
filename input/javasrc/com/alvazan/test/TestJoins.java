package com.alvazan.test;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.PartSecurity;
import com.alvazan.test.db.PartitionedTrade;

public class TestJoins {

	private static final Logger log = LoggerFactory.getLogger(TestJoins.class);
	
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
	public void testEmpty() {}
	
	//@Test
	public void testJoin() {
		putEntities();
		
		List<PartitionedTrade> trades = PartitionedTrade.findInNullPartition(mgr, 4, 6);
		
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
		PartitionedTrade trade = new PartitionedTrade();
		trade.setSecurity(sec);
		trade.setNumShares(5);
		
		PartitionedTrade trade2 = new PartitionedTrade();
		trade.setSecurity(sec);
		trade.setNumShares(6);
		
		PartitionedTrade trade3 = new PartitionedTrade();
		trade.setSecurity(sec3);
		trade.setNumShares(5);
		
		
	}
}
