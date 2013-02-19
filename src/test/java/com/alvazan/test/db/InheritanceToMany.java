package com.alvazan.test.db;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;

@NoSqlEntity
public class InheritanceToMany {

	@NoSqlId
	private String id;
	
	@NoSqlOneToMany(keyFieldForMap="lastName")
	private final Map<String, InheritanceSuper> nameToEntity = new HashMap<String, InheritanceSuper>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<String, InheritanceSuper> getNameToEntity() {
		return nameToEntity;
	}

//	public void setNameToEntity(Map<String, InheritanceSuper> nameToEntity) {
//		this.nameToEntity = nameToEntity;
//	}

	public void addEntity(InheritanceSuper entity) {
		nameToEntity.put(entity.getLastName(), entity);
	}
	
}
