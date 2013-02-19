package com.alvazan.orm.api.base;

public class Indexing {

	private static ThreadLocal<Boolean> forced = new ThreadLocal<Boolean>();
	
	public static void setForcedIndexing(boolean isForced) {
		forced.set(isForced);
	}
	
	public static boolean isForcedIndexing() {
		Boolean bool = forced.get();
		if(bool == null)
			return false;
		return bool;
	}
}
