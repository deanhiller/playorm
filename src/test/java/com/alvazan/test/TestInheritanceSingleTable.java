package com.alvazan.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.test.db.InheritanceSub1;
import com.alvazan.test.db.InheritanceSub2;
import com.alvazan.test.db.InheritanceSuper;
import com.alvazan.test.db.InheritanceToMany;
import com.alvazan.test.db.InheritanceToManySpecific;
import com.alvazan.test.db.InheritanceToOne;
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
	public void testSpecificQuery() {
		InheritanceSub1 common = new InheritanceSub1();
		common.setName("dean");
		common.setDiff("diff");
		common.setNum(56);
		mgr.put(common);
		
		InheritanceSub2 toMany = new InheritanceSub2();
		toMany.setDescription("dean");
		toMany.setNum(78);
		toMany.setNumBalls(33);
		mgr.put(toMany);
		
		mgr.flush();
		
		Cursor<KeyValue<InheritanceSub1>> results = InheritanceSub1.findAll(mgr);
		int size = 0;
		InheritanceSub1 sub1 = null;
		while(results.next()) {
			sub1 = results.getCurrent().getValue();
			size++;
		}
		Assert.assertEquals(1, size);
		Assert.assertEquals(common.getDiff(), sub1.getDiff());
	}
	
	@Test
	public void testQueryWithParam() {
		InheritanceSub1 common = new InheritanceSub1();
		common.setName("dean");
		common.setDiff("diff");
		common.setNum(56);
		mgr.put(common);
		
		InheritanceSub2 toMany = new InheritanceSub2();
		toMany.setDescription("dean");
		toMany.setNum(78);
		toMany.setNumBalls(33);
		mgr.put(toMany);
		
		mgr.flush();
		
		InheritanceSub1 result = InheritanceSub1.findByName(mgr, common.getName());
		Assert.assertNotNull(result);
		Assert.assertEquals(common.getDiff(), result.getDiff());
	}
	
	@Test
	public void testBasicMultipleClasses() {
		InheritanceSub1 common = new InheritanceSub1();
		common.setName("xxxx");
		common.setDiff("diff");
		common.setNum(56);
		mgr.put(common);
		
		InheritanceSub2 toMany = new InheritanceSub2();
		toMany.setDescription("werew");
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
		Assert.assertEquals(toMany.getDescription(), ((InheritanceSub2)many).getDescription());
	}
	
	@Test
	public void testBasicRemoveWithMultipleClasses() {
		InheritanceSub1 common = new InheritanceSub1();
		common.setName("xxxx");
		common.setDiff("diff");
		common.setNum(56);
		mgr.put(common);
		
		InheritanceSub2 toMany = new InheritanceSub2();
		toMany.setDescription("werew");
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
		Assert.assertEquals(toMany.getDescription(), ((InheritanceSub2)many).getDescription());
		
		mgr.remove(abs);
		abs = mgr.find(InheritanceSuper.class, common.getId());
		Assert.assertNull(abs);
		
		mgr.remove(many);
		many = mgr.find(InheritanceSuper.class, toMany.getId());
		Assert.assertNull(many);
	}

	@Test
	public void testToManyRelatipnshipSpecific() {
		InheritanceSub1 common = new InheritanceSub1();
		common.setLastName("hiller");
		common.setName("xxxx");
		common.setDiff("diff");
		common.setNum(56);
		mgr.put(common);
		
		InheritanceSub1 common2 = new InheritanceSub1();
		common2.setLastName("were");
		common2.setName("wwww");
		common2.setDiff("wwww");
		common2.setNum(56);
		mgr.put(common2);
		
		InheritanceToManySpecific ent1 = new InheritanceToManySpecific();
		ent1.addSomething(common);
		ent1.addSomething(common2);
		
		mgr.put(ent1);
		
		mgr.flush();
		
		InheritanceToManySpecific result1 = mgr.find(InheritanceToManySpecific.class, ent1.getId());
		Assert.assertEquals(2, result1.getInheritance().size());
		InheritanceSub1 sub1 = result1.getInheritance().get(0);
		//read the other row in
		sub1.getName();
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
	
	@Test
	public void testToOneRelationship() {
		InheritanceSub1 sub1 = new InheritanceSub1();
		sub1.setLastName("hiller");
		sub1.setName("xxxx");
		sub1.setDiff("diff");
		sub1.setNum(56);
		mgr.put(sub1);
		
		InheritanceSub2 sub2 = new InheritanceSub2();
		sub2.setLastName("smith");
		sub2.setDescription("werew");
		sub2.setNum(78);
		sub2.setNumBalls(33);
		mgr.put(sub2);
		
		InheritanceToOne ent1 = new InheritanceToOne();
		ent1.setInheritance(sub1);
		
		InheritanceToOne ent2 = new InheritanceToOne();
		ent2.setInheritance(sub2);
		
		mgr.put(ent1);
		mgr.put(ent2);
		
		mgr.flush();
		
		InheritanceToOne result1 = mgr.find(InheritanceToOne.class, ent1.getId());
		Assert.assertTrue(result1.getInheritance() instanceof InheritanceSub1);
		Assert.assertEquals(sub1.getNum(), result1.getInheritance().getNum());
		InheritanceSub1 subResult1 = (InheritanceSub1) result1.getInheritance();
		Assert.assertEquals(sub1.getDiff(), subResult1.getDiff());
		Assert.assertEquals(sub1.getName(), subResult1.getName());
		
		
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
		toMany.setDescription("werew");
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
		Assert.assertEquals(toMany.getDescription(), ((InheritanceSub2)sub2).getDescription());
	}
}
