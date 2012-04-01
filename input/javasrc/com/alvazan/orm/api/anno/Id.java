package com.alvazan.orm.api.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.alvazan.orm.api.spi.KeyGenerator;
import com.alvazan.orm.api.spi.UniqueKeyGenerator;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Id {
    @SuppressWarnings("rawtypes")
	Class targetEntity() default void.class;

	Class<? extends KeyGenerator> generation() default UniqueKeyGenerator.class;
}

