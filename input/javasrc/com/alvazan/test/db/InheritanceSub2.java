package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;

@NoSqlDiscriminatorColumn(value="sub2")
public class InheritanceSub2 extends InheritanceSuper {

	private String description;
	private int numBalls;
	
	public String getDescription() {
		return description;
	}
	public void setDescription(String name) {
		this.description = name;
	}
	public int getNumBalls() {
		return numBalls;
	}
	public void setNumBalls(int numBalls) {
		this.numBalls = numBalls;
	}
	
}
