package com.alvazan.test;

import com.alvazan.orm.api.Bootstrap;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;

public class TestBasic {

	public void setup() {
		NoSqlEntityManagerFactory factory = Bootstrap.getSingleton();
	}
}
