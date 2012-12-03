package com.alvazan.test;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.Query;
import com.alvazan.test.db.PartitionedTrade;
import com.alvazan.test.db.PartAccount;
import com.alvazan.test.db.PartitionedSingleTrade;

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
	public void testSinglePartitionedByAnnotationJQL() {
		PartitionedSingleTrade t = new PartitionedSingleTrade();
		t.setSecurityName("xyz");
		t.setNumber(5);
		t.setUnique(1);
		mgr.put(t);
		
		PartitionedSingleTrade t2 = new PartitionedSingleTrade();
		t2.setSecurityName("abc");
		t2.setNumber(5);
		t2.setUnique(2);
		mgr.put(t2);
		
		PartitionedSingleTrade t3 = new PartitionedSingleTrade();
		t3.setSecurityName(null);
		t3.setNumber(5);
		t3.setUnique(3);
		mgr.put(t3);
		
		mgr.flush();
		
		//The Query is 
		//  "PARTITIONS e(:partitionId) select * FROM TABLE as e WHERE e.number = :number"
		
		Query<PartitionedSingleTrade> query1 = mgr.createNamedQuery(PartitionedSingleTrade.class, "findByNumber");
		query1.setParameter("partitionId", t.getSecurityName());
		query1.setParameter("number", 5);
		PartitionedSingleTrade trade1 = query1.getSingleObject();
		Assert.assertEquals(t.getUnique(), trade1.getUnique());
		
		//The Query is 
		//  "PARTITIONS e(:partitionId) select * FROM TABLE as e WHERE e.number = :number"
		
		Query<PartitionedSingleTrade> query2 = mgr.createNamedQuery(PartitionedSingleTrade.class, "findByNumber");
		query2.setParameter("partitionId", null);
		query2.setParameter("number", 5);
		PartitionedSingleTrade trade3FromNullPartition = query2.getSingleObject();
		Assert.assertEquals(t3.getUnique(), trade3FromNullPartition.getUnique());
	}
	
	@Test
	public void testPartitioning() {
		PartAccount acc = new PartAccount();
		acc.setBusinessName("biz");
		acc.setSomeNumber(5);
		
		PartAccount acc2 = new PartAccount();
		acc.setBusinessName("biz2");
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
		mgr.put(trade3);
		mgr.put(trade4);
		
		mgr.flush();
		
		mgr.put(trade2);

		mgr.flush();
		
		List<PartitionedTrade> tradesInAcc1Partition = PartitionedTrade.findByUnique(mgr, trade.getUniqueColumn(), acc);
		
		Assert.assertEquals(2, tradesInAcc1Partition.size());
		
	}
}
