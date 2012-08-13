package com.alvazan.orm.api.base.util;

import java.util.List;

public interface PlayCallback {

	List<Class> getClassesToScan();

	ClassLoader getClassLoader();

	Object getCurrentRequest();
}
