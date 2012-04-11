package com.alvazan.orm.impl.bindings;

import com.google.inject.Binder;
import com.google.inject.Module;

public class InMemoryBinding implements Module {

	@Override
	public void configure(Binder binder) {
		//binder.bind(NoSqlRawSession.class).to(NoSqlSessionInMemory.class)
	}

}
