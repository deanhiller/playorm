package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;

@NoSqlDiscriminatorColumn(value="sub2")
public class InheritanceSub2 extends InheritanceSuper {

	private String name;
	private int numBalls;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getNumBalls() {
		return numBalls;
	}
	public void setNumBalls(int numBalls) {
		this.numBalls = numBalls;
	}
	
}
