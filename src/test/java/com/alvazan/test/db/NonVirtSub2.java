package com.alvazan.test.db;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;

@NoSqlDiscriminatorColumn(value="sub2")
public class NonVirtSub2 extends NonVirtSuper {

	private String description;
	@NoSqlIndexed
	private int numBalls;
	
	@NoSqlIndexed
	private String color;
	
	@NoSqlIndexed
	private String color2;
	
	@NoSqlIndexed
	private String color3;
	
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
