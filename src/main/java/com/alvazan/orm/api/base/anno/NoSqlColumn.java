package com.alvazan.orm.api.base.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.alvazan.orm.api.z5api.NoConversion;
import com.alvazan.orm.api.z8spi.conv.Converter;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface NoSqlColumn {
    String columnName() default ""; 

    /**
     * Unused at this point but eventually we may allow fields that have are declared like so
     * 
     * private Car car;
     * 
     *  but targetEntity may specify a subtype like Honda such that we would create a Honda object
     *  and put it into the Car field here.  I am not sure if this would be used or not so we
     *  have left it unimplemented
     * @return
     */
	@SuppressWarnings("rawtypes")
	Class targetEntity() default void.class;
	
	/**
	 * Deprecated: Use @NoSqlConverter instead!!!
	 * 
	 * You can supply your own converter for a field here which will override all
	 * standard conversions even for primitives.  You just translate back and
	 * forth from the byte[] for us and we wire that in.
	 * @return
	 */
	@Deprecated
	Class<? extends Converter> customConverter() default NoConversion.class;
}

