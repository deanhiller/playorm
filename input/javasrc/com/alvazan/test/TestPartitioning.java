package com.alvazan.test;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.exc.ChildWithNoPkException;
import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.test.db.Account;
import com.alvazan.test.db.Activity;
import com.alvazan.test.db.PartitionAccount;
import com.alvazan.test.db.PartitionTrade;

public class TestPartitioning {

	private static final Logger log = LoggerFactory.getLogger(TestPartitioning.class);
	
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
		other.clearDatabase();
	}
	
	@Test
	public void testEmpty() {}
	
	public void testPartitioning() {
		PartitionAccount acc = new PartitionAccount();
		acc.setIsActive(true);
		acc.setSomeNumber(5);
		
		PartitionAccount acc2 = new PartitionAccount();
		acc.setIsActive(false);
		acc.setSomeNumber(6);
		
		PartitionTrade trade = new PartitionTrade();
		trade.setAccount(acc);
		trade.setSecurityName("qwer");
		trade.setUniqueColumn("first");
		
		PartitionTrade trade2 = new PartitionTrade();
		trade2.setAccount(acc);
		trade2.setSecurityName("asdf");
		trade2.setUniqueColumn("first");
		
		PartitionTrade trade3 = new PartitionTrade();
		trade3.setAccount(acc);
		trade3.setSecurityName("asdf");
		trade3.setUniqueColumn("asdfdsf");
		
		PartitionTrade trade4 = new PartitionTrade();
		trade4.setAccount(acc2);
		trade4.setSecurityName("asdf");
		trade4.setUniqueColumn("asdfdsf");
		
		mgr.put(acc);
		mgr.put(acc2);
		mgr.put(trade);
		mgr.put(trade2);
		mgr.put(trade3);
		mgr.put(trade4);

		Index<PartitionTrade> index = mgr.getIndex(PartitionTrade.class, "account", acc);
		Query<PartitionTrade> query = index.getNamedQuery("findByUnique");
		query.setParameter("unique", trade.getUniqueColumn());
		List<PartitionTrade> tradesInAcc1Partition = query.getResultList();
		
		Assert.assertEquals(2, tradesInAcc1Partition.size());
		
	}
}
