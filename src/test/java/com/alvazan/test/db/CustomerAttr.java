package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlEntity;

@NoSqlEntity
public class CustomerAttr extends DGPrefixedAttribute {
	private int roll;

	public int getRoll() {
		return roll;
	}

	public void setRoll(int roll) {
		this.roll = roll;
	}

}
