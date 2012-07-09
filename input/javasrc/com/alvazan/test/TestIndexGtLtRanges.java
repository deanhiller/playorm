package com.alvazan.test;

import org.junit.BeforeClass;
import org.junit.Test;

import com.alvazan.orm.api.base.AbstractBootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Activity;

public class TestIndexGtLtRanges {

	private static NoSqlEntityManager mgr;
	private static Index<Activity> index;

	@BeforeClass
	public static void setup() {
		NoSqlEntityManagerFactory factory = AbstractBootstrap.create(DbTypeEnum.IN_MEMORY);
		factory.setup(null, "com.alvazan.test.db");
		mgr = factory.createEntityManager();
		index = setupRecords();
	}

	@Test
	public void testBasicString() {
		//TODO: test some ranges out here
	}

	private static Index<Activity> setupRecords() {
		Activity act = new Activity();
		act.setName("hello");
		act.setMyFloat(5.65f);
		act.setUniqueColumn("notunique");
		act.setNumTimes(5);
		act.setIsCool(true);
		mgr.put(act);
		
		Index<Activity> index = mgr.getIndex(Activity.class, "/activity/byaccount/account1");
		index.addToIndex(act);
		
		mgr.flush();
		return index;
	}

}
