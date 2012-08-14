package com.alvazan.orm.api.base.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.alvazan.orm.api.base.spi.KeyGenerator;
import com.alvazan.orm.api.base.spi.NoConversion;
import com.alvazan.orm.api.base.spi.UniqueKeyGenerator;
import com.alvazan.orm.api.spi1.meta.conv.Converter;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface NoSqlId {
//    @SuppressWarnings("rawtypes")
//	Class targetEntity() default void.class;

	Class<? extends KeyGenerator> generation() default UniqueKeyGenerator.class;
	
	/**
	 * true to use the KeyGenerator supplied in generation attribute, false to 
	 * manually set the key value every time.
	 */
	boolean usegenerator() default true;
	
	/**
	 * You can supply your own converter for a field here which will override all
	 * standard conversions even for primitives.  You just translate back and
	 * forth from the byte[] for us and we wire that in.
	 * @return
	 */
	Class<? extends Converter> customConverter() default NoConversion.class;
	
	String columnName() default "";
}

