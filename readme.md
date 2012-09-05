## Support

``` We answer questions on stackoverflow, so just tag the question with "playOrm".```

## PlayOrm Feature List

* Scalabla JQL(SJQL) supported which is modified JQL that scales(SQL doesn't scale well)
* Partitioning so you can query a one trillion row table in just ms with SJQL(Scalable Java Query Language)
* Typical query support of <=, <, >, >= and = and no limitations here
* Typical query support of AND and OR as well as parenthesis 
* Inner Join support (Must keep your very very large tables partitioned so you get very fast access times here)
* OneToMany, ManyToMany, OneToOne, and ManyToOne but the ToMany's are nosql fashion not like RDBMS
* support of a findAll(Class c, List<Object> keys) as is typical in nosql to parallel the reads
* Inheritance class heirarchy in one table is supported like hibernate
* flush() If any failures in your thread happen, nothing is written to cassandra as it all is written on flush
* first level read cache
* Automatically creates ColumnFamilies at runtime
* Includes it's own in-memory database for TDD in your unit tests!!!!!
* All primitive types converterd to stored as smallest possibly unit so long is stored with only one byte IF that long is between -128 and 127 so using playorm, you automatically store everything as the smallest possibly units and all your indexes and queries still work
* logging interface below the first level cache so you can see the raw operations on cassandra and optimize just like when you use hibernate's logging
* A raw interface using only BigDecimal, BigInteger, and String types which is currently used to upload user defined datasets through a web interface(and we wire that into generating meta data so they can ad-hoc query on the nosql system)
* An ad-hoc query interface that can query on any table that was from an Entity object.  To us on other tables, you can also code up and save DboTableMeta objects and the ad-hoc query interface gets you query support into those tables

### Features soon to be added
* WAY better support for @Embedded and putting Map<String, Type> in your entities for wide row support
* TONS of documentation is in the works due out 9/16/12
* Left outer Join support coming soon
* More work around solid ad-hoc tool 

### Flush - 
A major problem with people using Hector and Astyanax is they do not queue up all their writes/mutations for the end of the transation so if something fails in the middle, ONLY HALF of the data is written and you end up with a corrupt database.  The flush method on PlayOrm is what pushes all your persists down in one shot so it sort of sends it as a unit of work(NOT a transation).  If there is an exception before the flush, nothing gets written to the nosql store.

### Note on Test Driven Development

We believe TDD to be very important and believe in more component testing than Functional or QA testing(testing that includes testing more code than unit tests typically do, but less code then a full QA test so we can simulate errors that QA cannot).  For this reason, the first priority of this project is to always have an in-memory implementation of indexes, databases, etc. so that you can use a live database during your unit testing that is in-memory and easily wipe it by just creating another one.

### Virtual Databases and Index Partitioning

nosqlORM wants to be the first ORM layer with full indexing AND joins in a noSQL environment.  What has not caught on yet in the nosql world is that you CAN do joins with select statements but need to do so in virtual databases(which I will explain below).  The problem with scalability on old RDBMS systems is really the indexing is not broken up.  If you want to scale, you want to be able to grow a table to 1 trillion rows BUT you can't have one index for that table.  You can however have 1 billion indexes and scale just fine.  Within this indexing framework, you can do joins.  Or within these virtual datbase views you can do joins(and sometimes across the virtual views as well)

Basically, it is sort of like having virtual databases.  We explored this concept and succeeded in two spaces already which is why we are developing this solution.  First imagine a simple system of performance accounting and reporting and you have an Account table with Activities in your RDBMS and those activities also have securities.  In this model there are two views of the virtual datbase.

The first one is that we had 1 billion rows in the activity table and 1 million accounts so we created one million indexes.  Let's say we also have a small 50 row table of securityType.  We can get the index we want to query like so

```
//NOTE "account" refers to the field that we partitioned by and account1 IS the partition we
//are interested in so it will only give us trades that have account1 as their account field.
Partition partition = mgr.createPartition(Activity.class, "account", account1);
Query query = partition.getNamedQuery("queryByValue");
query.setParameter("value", value);
query.setMaxResults(100);
query.setPageNumber(0);
List<Activity> activities = query.getResultList();
```
Notice that this scales just fine BUT leaves us in the old school convenience of RDBMS solutions making RDBMS solutions MUCH MUCH easier to scale to infinite nodes.  That is one virtual view of the data. Another virtual view of the data would be by security like so

```
//NOTE: here, we also had a view that was partitioned by security and are interested in the
//security1 partition(ie. all Activities that have security1 in their security field
Partition partition = mgr.getIndex(Activity.class, "security", security1);
Query query = partition.createNamedQuery("queryByValue");
query.setParameter("value", value);
query.setMaxResults(100);
query.setPageNumber(0);
List<Activity> activities = query.getResultList();
```

This is another virtual view like looking into an RDBMS.

So what about the denormalization hype in noSQL?  Well, be careful.  I was on one project where one request was taking 80 seconds and by re-normalizing all their data and not repeating so much of it, I brought the query down to 200ms.  Behind the scenes the denormalization was causing 1 megabyte of data to be written on the request which normalization avoided.  Denormalization can be good but partitioning of your indexes and normalizing the data is yet another way to solve similar issues.  I do NOT encourage you to always normalize, just sometimes.  Do be careful of having one small table that can be a hotspot though.  Like anything, this is a tool and needs to be used correctly.  Like hibernate, it can be used wrong.

## What Joins would look like

Currently working on joins 8/18/12

(We currently don't support joins but are in the process of adding them)
Taking our previous example of the million indexes we have by acount or the huge amount of indexes we have by security, let's say we have a table with no more than 10,000 rows called ActivityTypeInfo which has a column vendor as well as many other columns.  Let's also say our Activity has a column ActivityTypeInfoId for our join.  Now we could do a join like so with any of those million indexes by account or and of the security indexes like so

```
PartitionInfo info = new PartitionInfo(ActivityTypeInfo.class); //This table is so small, it only has one partition
//NOTE: mgr.getPartition takes the primary index first!!! and then a varargs of the indexes we need to join with after that
Partition partition = mgr.getPartition(Activity.class, "/activity/bysecurity/"+security.getId(), info);
Query query = partition.getNamedQuery("findWithJoinQuery");
query.setParameter("activityValue", 5); //This is a value in our Activity table
query.setParameter("vendor", "companyX"); //This is a value in our ActivityTypeInfo table
List<Activity> activity = query.getResultList();
```


## Another Use-Case

Another perfect use-case for virtual databases is you are selling to small businesses so you basically design a table called Small Business and each business has tons of clients so you have a SmallBizClients table.  Those clients then have relationships with other clients or products that the small business has so maybe a smallbizproducts table and clientrelationships tables.  You are now good to go and we work on our partitioning of the indexes to create the virtual databases.  Clearly we would want to partition the SmallBizClients table so we can have trillions of clients and billions of small busineses.  We can then grab the index for that small business and start querying the smallbizclients table for that small business.

## TODO
* adding JDBC such legacy apps can work with non-partitioned tables AND other apps can be modified to prefix queries with the partition ids and still use JDBC so they can just make minor changes to their applications(ie. keep track of partition ids)!!!  NOTE: This is especially useful for reporting tools
