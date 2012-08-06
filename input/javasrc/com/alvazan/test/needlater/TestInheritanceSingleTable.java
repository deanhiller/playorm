package com.alvazan.test.needlater;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.spi2.DboAbstractColumnMeta;
import com.alvazan.orm.api.spi2.DboColumnCommonMeta;
import com.alvazan.orm.api.spi2.DboColumnToManyMeta;
import com.alvazan.test.FactorySingleton;

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

	//@Test
	public void testBasicMultipleClasses() {
		DboColumnCommonMeta common = new DboColumnCommonMeta();
		common.setColumnName("generic");
		common.setColumnType(String.class.getName());
		mgr.put(common);
		
		DboColumnToManyMeta toMany = new DboColumnToManyMeta();
		toMany.setColumnName("many");
		mgr.put(toMany);
		mgr.flush();
		
		DboAbstractColumnMeta abs = mgr.find(DboAbstractColumnMeta.class, common.getId());
		Assert.assertEquals(DboColumnCommonMeta.class, abs.getClass());
		Assert.assertEquals(common.getColumnType(), ((DboColumnCommonMeta)abs).getColumnType());
		
		DboAbstractColumnMeta many = mgr.find(DboAbstractColumnMeta.class, toMany.getId());
		Assert.assertEquals(DboColumnToManyMeta.class, many.getClass());
	}

}
