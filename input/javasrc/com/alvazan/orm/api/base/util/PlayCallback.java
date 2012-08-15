package com.alvazan.orm.api.base.util;

import java.util.List;

public interface PlayCallback {

	@SuppressWarnings("rawtypes")
	List<Class> getClassesToScan();

	ClassLoader getClassLoader();

	Object getCurrentRequest();
}
