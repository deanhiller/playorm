package com.alvazan.test.db;

import java.util.ArrayList;
import java.util.Collection;

import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;

@NoSqlEntity
public class ForCollection {

	@NoSqlId
	private String id;
	
	private String name;

	@NoSqlOneToMany(entityType=Activity.class)
	private Collection<Activity> activities = new ArrayList<Activity>();
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Collection<Activity> getActivities() {
		return activities;
	}

}
