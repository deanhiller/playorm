package com.example.db;


import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;

@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findByName", query="select u from TABLE as u where :name = u.name"),
	@NoSqlQuery(name="findAll", query="select * FROM TABLE as u"),
	@NoSqlQuery(name="findBetween", query="select u from TABLE as u where u.age > :start and u.age < :end")
})
public class User {

	@NoSqlId
	private String id;
	
	@NoSqlIndexed
	private String name;

	@NoSqlIndexed
	private int age;
	
	private String lastName;
	
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}


	public static User findByName(NoSqlEntityManager mgr, String name) {
		Query<User> query = mgr.createNamedQuery(User.class, "findByName");
		query.setParameter("name", name);
		return query.getSingleObject();
	}
	
	public static List<User> findByAge(NoSqlEntityManager mgr, int start, int end) {
		Query<User> query = mgr.createNamedQuery(User.class, "findBetween");
		query.setParameter("start", start);
		query.setParameter("end", end);
		return query.getResultList(0, null);
	}
	public static List<User> findAll(NoSqlEntityManager mgr, int batchSize) {
		Query<User> query = mgr.createNamedQuery(User.class, "findAll");
		query.setBatchSize(batchSize);
		return query.getResultList(0, null);
	}
}
