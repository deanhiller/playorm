package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlEntity;

@NoSqlEntity
public class Localization extends DGPrefixedAttribute {
	private String add;

	public String getAdd() {
		return add;
	}

	public void setAdd(String add) {
		this.add = add;
	}

}
