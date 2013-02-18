package com.alvazan.orm.api.base.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.alvazan.orm.api.z8spi.conv.Converter;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface NoSqlConverter {
	/**
	 * You can supply your own converter for a field here which will override all
	 * standard conversions even for primitives.  You just translate back and
	 * forth from the byte[] for us and we wire that in.
	 * @return An object of <code>Class</code> which extends <code>Converter</code> 
	 */
	Class<? extends Converter> converter();
}

