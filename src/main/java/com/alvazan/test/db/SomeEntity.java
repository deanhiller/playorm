package com.alvazan.test.db;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;

@NoSqlEntity
public class SomeEntity {

	@NoSqlId
	private String id;
	
	private String name;

	@NoSqlOneToMany(keyFieldForMap="name")
	private Map<String, Activity> activities = new HashMap<String, Activity>();
	
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

	public void putActivity(Activity act) {
		activities.put(act.getName(), act);
	}

	public Activity getActivity(String name) {
		return activities.get(name);
	}
	
	public Collection<Activity> getActivities() {
		return activities.values();
	}

	public void remove(String name2) {
		activities.remove(name2);
	}
}
