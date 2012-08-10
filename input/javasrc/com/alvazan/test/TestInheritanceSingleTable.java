package com.alvazan.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.InheritanceRelation;
import com.alvazan.test.db.InheritanceSub1;
import com.alvazan.test.db.InheritanceSub2;
import com.alvazan.test.db.InheritanceSuper;

public class TestInheritanceSingleTable {

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
		other.clearDbAndIndexesIfInMemoryType();
	}

	@Test
	public void testBasicMultipleClasses() {
		InheritanceSub1 common = new InheritanceSub1();
		common.setName("xxxx");
		common.setDiff("diff");
		common.setNum(56);
		mgr.put(common);
		
		InheritanceSub2 toMany = new InheritanceSub2();
		toMany.setName("werew");
		toMany.setNum(78);
		toMany.setNumBalls(33);
		mgr.put(toMany);
		mgr.flush();
		
		InheritanceSuper abs = mgr.find(InheritanceSuper.class, common.getId());
		Assert.assertTrue(InheritanceSub1.class.isAssignableFrom(abs.getClass()));
		Assert.assertEquals(common.getNum(), abs.getNum());
		Assert.assertEquals(common.getName(), ((InheritanceSub1)abs).getName());
		Assert.assertEquals(common.getDiff(), ((InheritanceSub1)abs).getDiff());
		
		InheritanceSuper many = mgr.find(InheritanceSuper.class, toMany.getId());
		Assert.assertTrue(InheritanceSub2.class.isAssignableFrom(many.getClass()));
		Assert.assertEquals(toMany.getNum(), many.getNum());
		Assert.assertEquals(toMany.getName(), ((InheritanceSub2)many).getName());
	}

	@Test
	public void testToManyRelationship() {
		InheritanceSub1 common = new InheritanceSub1();
		common.setLastName("hiller");
		common.setName("xxxx");
		common.setDiff("diff");
		common.setNum(56);
		mgr.put(common);
		
		InheritanceSub2 toMany = new InheritanceSub2();
		toMany.setLastName("smith");
		toMany.setName("werew");
		toMany.setNum(78);
		toMany.setNumBalls(33);
		mgr.put(toMany);
		
		InheritanceRelation rel = new InheritanceRelation();
		rel.addEntity(common);
		rel.addEntity(toMany);
		mgr.put(rel);
		
		mgr.flush();
		
		InheritanceRelation newRel = mgr.find(InheritanceRelation.class, rel.getId());
		InheritanceSuper sub1 = newRel.getNameToEntity().get(common.getLastName());
		Assert.assertEquals(common.getId(), sub1.getId());
		Assert.assertEquals(common.getName(), ((InheritanceSub1)sub1).getName());
		Assert.assertEquals(common.getNum(), sub1.getNum());
		
		InheritanceSuper sub2 = newRel.getNameToEntity().get(toMany.getLastName());
		Assert.assertEquals(toMany.getId(), sub2.getId());
		Assert.assertEquals(toMany.getNum(), sub2.getNum());
		Assert.assertEquals(toMany.getName(), ((InheritanceSub2)sub2).getName());
	}
}
