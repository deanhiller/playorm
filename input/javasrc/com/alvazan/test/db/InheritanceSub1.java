package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;

@NoSqlDiscriminatorColumn(value="sub1")
public class InheritanceSub1 extends InheritanceSuper {

	private String name;
	
	private String diff;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDiff() {
		return diff;
	}

	public void setDiff(String diff) {
		this.diff = diff;
	}
	
}
