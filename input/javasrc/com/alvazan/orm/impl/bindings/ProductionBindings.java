package com.alvazan.orm.impl.bindings;

import com.google.inject.Binder;
import com.google.inject.Module;

public class ProductionBindings implements Module {

	/**
	 * Mostly empty because we bind with annotations when we can.  Only third party bindings will
	 * end up in this file because we can't annotate third party objects
	 */
	@Override
	public void configure(Binder binder) {
		
	}

}
