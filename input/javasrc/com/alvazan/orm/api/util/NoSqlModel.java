package com.alvazan.orm.api.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import play.data.binding.BeanWrapper;
import play.data.binding.Binder;
import play.data.binding.ParamNode;
import play.data.binding.ParamNode.RemovedNode;
import play.data.validation.Validation;
import play.exceptions.UnexpectedException;

import com.alvazan.orm.api.base.MetaLayer;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.anno.NoSqlManyToMany;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;
import com.alvazan.orm.api.base.anno.NoSqlOneToOne;

public class NoSqlModel {
    public static <T> T create(ParamNode rootParamNode, String name, Class<T> type, Annotation[] annotations) {
        try {
            Constructor<T> c = type.getDeclaredConstructor();
            c.setAccessible(true);
            T model = c.newInstance();
            return edit(rootParamNode, name, model, annotations);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("rawtypes")
	public static <T> T edit(ParamNode rootParamNode, String name, T o, Annotation[] annotations) {
        ParamNode paramNode = rootParamNode.getChild(name, true);
        // #1195 - Needs to keep track of whick keys we remove so that we can restore it before
        // returning from this method.
        List<ParamNode.RemovedNode> removedNodesList = new ArrayList<ParamNode.RemovedNode>();
        try {
            BeanWrapper bw = new BeanWrapper(o.getClass());
            // Start with relations
            Set<Field> fields = new HashSet<Field>();
            Class clazz = o.getClass();
            while (!clazz.equals(Object.class)) {
                Collections.addAll(fields, clazz.getDeclaredFields());
                clazz = clazz.getSuperclass();
            }
            for (Field field : fields) {
            	processField(field, paramNode, bw, removedNodesList, o);
            }
            ParamNode beanNode = rootParamNode.getChild(name, true);
            Binder.bindBean(beanNode, o, annotations);
            return (T) o;
        } catch (Exception e) {
            throw new UnexpectedException(e);
        } finally {
            // restoring changes to paramNode
            ParamNode.restoreRemovedChildren( removedNodesList );
        }
    }

	@SuppressWarnings("rawtypes")
	private static void processField(Field field, ParamNode paramNode, BeanWrapper bw, List<RemovedNode> removedNodesList, Object o) {
    	NoSqlEntityManager em = NoSql2.em();
    	MetaLayer meta = em.getMeta();
    	
        boolean isEntity = false;
        Class<?> relation = null;
        boolean multiple = false;
        //
        if (field.isAnnotationPresent(NoSqlOneToOne.class) || field.isAnnotationPresent(NoSqlManyToOne.class)) {
            isEntity = true;
            relation = field.getType();
        } else if (field.isAnnotationPresent(NoSqlOneToMany.class) || field.isAnnotationPresent(NoSqlManyToMany.class)) {
            Class fieldType = (Class) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
            isEntity = true;
            relation = fieldType;
            multiple = true;
        }

        if (!isEntity)
        	return;

        ParamNode fieldParamNode = paramNode.getChild(field.getName(), true);

        String keyName = meta.getKeyFieldName(relation);
    	String[] ids = fieldParamNode.getChild(keyName, true).getValues();
        if (multiple && Collection.class.isAssignableFrom(field.getType())) {
//        	Collection l = new ArrayList();
//        	if (SortedSet.class.isAssignableFrom(field.getType())) {
//        		l = new TreeSet();
//        	} else if (Set.class.isAssignableFrom(field.getType())) {
//        		l = new HashSet();
//        	}
        
        	//NOTE: for now we skip this
        	
        } else if(ids == null || ids.length == 0) {
        	return;
        } else if (!ids[0].equals("")) {
        	Object theId = meta.convertIdFromString(relation, ids[0]);
        	Object to = em.find(relation, theId);
        	if(to != null) {
        		edit(paramNode, field.getName(), to, field.getAnnotations());
        		// Remove it to prevent us from finding it again later
        		paramNode.removeChild( field.getName(), removedNodesList);
        		bw.set(field.getName(), o, to);
        	} else {
        		Validation.addError(fieldParamNode.getOriginalKey(), "validation.notFound", ids[0]);
        		// Remove only the key to prevent us from finding it again later
        		// This how the old impl does it..
        		fieldParamNode.removeChild(keyName, removedNodesList);
        		if (fieldParamNode.getAllChildren().size()==0) {
        			// remove the whole node..
        			paramNode.removeChild( field.getName(), removedNodesList);
        		}
        	}

        } else if (ids[0].equals("")) {
        	bw.set(field.getName(), o, null);
        	// Remove the key to prevent us from finding it again later
        	fieldParamNode.removeChild(keyName, removedNodesList);
        }
	}
}
