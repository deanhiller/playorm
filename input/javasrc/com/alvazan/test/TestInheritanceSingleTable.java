package com.alvazan.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.InheritanceSub1;
import com.alvazan.test.db.InheritanceSub2;
import com.alvazan.test.db.InheritanceSuper;
import com.alvazan.test.db.InheritanceToMany;
import com.alvazan.test.db.InheritanceToOneSpecific;

public class TestInheritanceSingleTable {

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
	public void testToOneRelatipnshipSpecific() {
		InheritanceSub1 common = new InheritanceSub1();
		common.setLastName("hiller");
		common.setName("xxxx");
		common.setDiff("diff");
		common.setNum(56);
		mgr.put(common);
		
		InheritanceToOneSpecific ent1 = new InheritanceToOneSpecific();
		ent1.setInheritance(common);
		
		mgr.put(ent1);
		
		mgr.flush();
		
		InheritanceToOneSpecific result1 = mgr.find(InheritanceToOneSpecific.class, ent1.getId());
		Assert.assertTrue(result1.getInheritance() instanceof InheritanceSub1);
		Assert.assertEquals(common.getNum(), result1.getInheritance().getNum());
		InheritanceSub1 subResult1 = (InheritanceSub1) result1.getInheritance();
		Assert.assertEquals(common.getDiff(), subResult1.getDiff());
		Assert.assertEquals(common.getName(), subResult1.getName());		
	}
	
	//Special case, not done with this one yet...
	//@Test
	public void testToOneRelationship() {
//		InheritanceSub1 common = new InheritanceSub1();
//		common.setLastName("hiller");
//		common.setName("xxxx");
//		common.setDiff("diff");
//		common.setNum(56);
//		mgr.put(common);
//		
//		InheritanceSub2 toMany = new InheritanceSub2();
//		toMany.setLastName("smith");
//		toMany.setName("werew");
//		toMany.setNum(78);
//		toMany.setNumBalls(33);
//		mgr.put(toMany);
//		
//		InheritanceToOne ent1 = new InheritanceToOne();
//		ent1.setInheritance(common);
//		
//		InheritanceToOne ent2 = new InheritanceToOne();
//		ent2.setInheritance(toMany);
//		
//		mgr.put(ent1);
//		mgr.put(ent2);
//		
//		mgr.flush();
//		
//		InheritanceToOne result1 = mgr.find(InheritanceToOne.class, ent1.getId());
//		Assert.assertTrue(result1.getInheritance() instanceof InheritanceSub1);
//		Assert.assertEquals(common.getNum(), result1.getInheritance().getNum());
//		InheritanceSub1 subResult1 = (InheritanceSub1) result1.getInheritance();
//		Assert.assertEquals(common.getDiff(), subResult1.getDiff());
//		Assert.assertEquals(common.getName(), subResult1.getName());
		
		
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
		
		InheritanceToMany rel = new InheritanceToMany();
		rel.addEntity(common);
		rel.addEntity(toMany);
		mgr.put(rel);
		
		mgr.flush();
		
		InheritanceToMany newRel = mgr.find(InheritanceToMany.class, rel.getId());
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
