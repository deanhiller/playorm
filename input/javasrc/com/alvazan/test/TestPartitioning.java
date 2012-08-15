package com.alvazan.test;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.Partition;
import com.alvazan.orm.api.base.Query;
import com.alvazan.test.db.PartAccount;
import com.alvazan.test.db.PartitionedTrade;

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
		other.clearDatabase();
	}
	
	@Test
	public void testEmpty() {}
	public void testPartitioning() {
		PartAccount acc = new PartAccount();
		acc.setIsActive(true);
		acc.setSomeNumber(5);
		
		PartAccount acc2 = new PartAccount();
		acc.setIsActive(false);
		acc.setSomeNumber(6);
		
		PartitionedTrade trade = new PartitionedTrade();
		trade.setAccount(acc);
		trade.setSecurityName("qwer");
		trade.setUniqueColumn("first");
		
		PartitionedTrade trade2 = new PartitionedTrade();
		trade2.setAccount(acc);
		trade2.setSecurityName("asdf");
		trade2.setUniqueColumn("first");
		
		PartitionedTrade trade3 = new PartitionedTrade();
		trade3.setAccount(acc);
		trade3.setSecurityName("asdf");
		trade3.setUniqueColumn("asdfdsf");
		
		PartitionedTrade trade4 = new PartitionedTrade();
		trade4.setAccount(acc2);
		trade4.setSecurityName("asdf");
		trade4.setUniqueColumn("asdfdsf");
		
		mgr.put(acc);
		mgr.put(acc2);
		mgr.put(trade);
		mgr.put(trade2);
		mgr.put(trade3);
		mgr.put(trade4);

		Partition<PartitionedTrade> index = mgr.getPartition(PartitionedTrade.class, "account", acc);
		Query<PartitionedTrade> query = index.getNamedQuery("findByUnique");
		query.setParameter("unique", trade.getUniqueColumn());
		List<PartitionedTrade> tradesInAcc1Partition = query.getResultList();
		
		Assert.assertEquals(2, tradesInAcc1Partition.size());
		
	}
}
