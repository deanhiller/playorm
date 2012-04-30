package com.alvazan.test;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.alvazan.orm.api.Bootstrap;
import com.alvazan.orm.api.DbTypeEnum;
import com.alvazan.orm.api.Index;
import com.alvazan.orm.api.NoSqlEntityManager;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;
import com.alvazan.test.db.Activity;

public class TestIndexBasic {

	private NoSqlEntityManagerFactory factory;

	@Before
	public void setup() {
		factory = Bootstrap.create(DbTypeEnum.IN_MEMORY);
		factory.setup(null, "com.alvazan.test.db");
	}
	
	@Test
	public void testSeparateIndexes() {
		NoSqlEntityManager mgr = factory.createEntityManager();

		//Activity has null reference to account
		Activity act = new Activity();
		act.setName("hello");
		act.setNumTimes(13);
		mgr.put(act);
		
		Index<Activity> index = mgr.getIndex(Activity.class, "/activity/byaccount/account1");
		index.addToIndex(act);
		
		Activity act1 = new Activity();
		act1.setName("hello");
		act1.setNumTimes(13);
		mgr.put(act1);
		
		Index<Activity> index2 = mgr.getIndex(Activity.class, "/activity/byaccount/account2");
		index2.addToIndex(act1);
		
		//flush the persists and the index modifications to the database and index storage 
		mgr.flush();
		
		//List<Activity> activities = Activity.findByGreaterThanNumTimes(index, 3);
		
		
	}

}
