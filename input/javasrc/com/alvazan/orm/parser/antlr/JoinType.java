package com.alvazan.orm.parser.antlr;

public enum JoinType {

	NONE, 
	/**
	 * SIDE_OUTER is either left or right, but is ALWAYS determined by the primary table in JoinInfo.java
	 * such that we don't care as the primary table is put in the primary so it could be left or right join
	 * but we get to treat them the same.
	 */
	LEFT_OUTER, OUTER, INNER;
	
}
