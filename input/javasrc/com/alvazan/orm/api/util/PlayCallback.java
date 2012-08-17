package com.alvazan.orm.api.util;

import java.util.List;

public interface PlayCallback {

	@SuppressWarnings("rawtypes")
	List<Class> getClassesToScan();

	ClassLoader getClassLoader();

	Object getCurrentRequest();
}
