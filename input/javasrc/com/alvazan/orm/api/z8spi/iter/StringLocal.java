package com.alvazan.orm.api.z8spi.iter;

public class StringLocal {

	private static final ThreadLocal<String> SPACES = new ThreadLocal<String>();
	
	private static String get() {
		String val = SPACES.get();
		if(val == null)
			return "";
		return val;
	}
	
	public static void set(int numSpaces) {
		String s = "";
		for(int i = 0; i < numSpaces; i++) {
			s+=" ";
		}
		SPACES.set(s);
	}

	public static String getAndAdd() {
		String tabs = get();
		set(tabs.length()+3);
		return "\n"+get();
	}

}
