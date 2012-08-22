package com.alvazan.test;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.AAPartitionedTrade;
import com.alvazan.test.db.PartAccount;

public class TestPartitioning {

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
	public void testPartitioning() {
		PartAccount acc = new PartAccount();
		acc.setBusinessName("biz");
		acc.setSomeNumber(5);
		
		PartAccount acc2 = new PartAccount();
		acc.setBusinessName("biz2");
		acc.setSomeNumber(6);
		
		AAPartitionedTrade trade = new AAPartitionedTrade();
		trade.setAccount(acc);
		trade.setSecurityName("qwer");
		trade.setUniqueColumn("first");
		
		AAPartitionedTrade trade2 = new AAPartitionedTrade();
		trade2.setAccount(acc);
		trade2.setSecurityName("asdf");
		trade2.setUniqueColumn("first");
		
		AAPartitionedTrade trade3 = new AAPartitionedTrade();
		trade3.setAccount(acc);
		trade3.setSecurityName("asdf");
		trade3.setUniqueColumn("asdfdsf");
		
		AAPartitionedTrade trade4 = new AAPartitionedTrade();
		trade4.setAccount(acc2);
		trade4.setSecurityName("asdf");
		trade4.setUniqueColumn("asdfdsf");
		
		mgr.put(acc);
		mgr.put(acc2);
		mgr.put(trade);
		mgr.put(trade3);
		mgr.put(trade4);
		
		mgr.flush();
		
		mgr.put(trade2);

		mgr.flush();
		
		List<AAPartitionedTrade> tradesInAcc1Partition = AAPartitionedTrade.findByUnique(mgr, trade.getUniqueColumn(), acc);
		
		Assert.assertEquals(2, tradesInAcc1Partition.size());
		
	}
}
