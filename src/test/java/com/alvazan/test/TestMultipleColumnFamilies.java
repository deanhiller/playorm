package com.alvazan.test;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z8spi.meta.DboColumnCommonMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;

public class TestMultipleColumnFamilies {

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
	public void testData(){
		DboDatabaseMeta metaDb = mgr.find(DboDatabaseMeta.class,DboDatabaseMeta.META_DB_ROWKEY);
		for (int i = 1; i <= 20; i++) {
			addMetaClassDboTest(metaDb, "Test" + i, "id"+i, "cat1", "mouse", "dog");
		}
		mgr.flush();
		NoSqlTypedSession session = mgr.getTypedSession();
		for (int j = 1; j < 20; j++) {
			for (int l = 1; l <= 100; l++) {
			TypedRow typedRow = session.createTypedRow("Test" + j);
			for(int k=1;k<100;k++){
			typedRow.addColumn("col"+k, "deano"+k);
			}
			typedRow.setRowKey("Row" + l+"/"+"Test" + j);
			session.put("Test" + j, typedRow);
			}
			session.flush();
		}
	}

	private DboTableMeta addMetaClassDboTest(DboDatabaseMeta metaDb, String name,String idFieldName, String ... columnField) {
		DboTableMeta meta=null;
		for(int j=1;j<20;j++){
		meta= new DboTableMeta();
		meta.setup(null, name, false);
		metaDb.addMetaClassDbo(meta);
				
		DboColumnIdMeta idMeta = new DboColumnIdMeta();
		idMeta.setup(meta,name+idFieldName, String.class, true);
		mgr.put(idMeta);
		}
		for(String field : columnField) {
			DboColumnCommonMeta fieldDbo = new DboColumnCommonMeta();
			fieldDbo.setup(meta, field, String.class, true, false);
			mgr.put(fieldDbo);
		}
		mgr.put(meta);
		return meta;
		
	}
	

}
