package com.alvazan.orm.api.z8spi.conv;

public class Precondition {

	public static void check(Object field, String name) {
		if(field == null)
			throw new IllegalArgumentException("parameter="+name+" is null and that is not allowed");
	}
}
