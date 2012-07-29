package com.alvazan.test.db;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.base.anno.OneToMany;

@NoSqlEntity
@NoSqlQuery(name="findById", query="select * FROM TABLE e where e.id=:id")
public class SomeEntity {

	@Id
	private String id;
	
	private String name;

	@OneToMany(entityType=Activity.class, keyFieldForMap="name")
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

	public static SomeEntity findByKey(Index<SomeEntity> index, String key) {
		Query<SomeEntity> query = index.getNamedQuery("findById");
		query.setParameter("id", key);
		return query.getSingleObject();
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
