PlayOrm Feature List

* Partitioning so you can query a one trillion row table in just ms with JQL(Java Query Language)
* OneToMany, ManyToMany, OneToOne, and ManyToOne but the ToMany's are nosql fashion not like RDBMS
* support of a findAll(Class c, List<Object> keys) as is typical in nosql to parallel the reads
* Inhertance class heirarchy in one table is supported like hibernate
* flush() If any failures in your thread happen, nothing is written to cassandra as it all is written on flush
* first level read cache

Flush - 
A major problem with people using Hector and Astyanax is they do not queue up all their writes/mutations for the end of the transation so if something fails in the middle, ONLY HALF of the data is written and you end up with a corrupt database.  The flush method on PlayOrm is what pushes all your persists down in one shot so it sort of sends it as a unit of work.  In typical nosql fashion of course, this is NOT a transaction, more like a unit of work.


Note on Test Driven Development

We believe TDD to be very important and believe in more component testing than Functional or QA testing(testing that includes testing more code than unit tests typically do, but less code then a full QA test so we can simulate errors that QA cannot).  For this reason, the first priority of this project is to always have an in-memory implementation of indexes, databases, etc. so that you can use a live database during your unit testing that is in-memory and easily wipe it by just creating another one.

Virtual Databases and Index Partitioning

nosqlORM wants to be the first ORM layer with full indexing AND joins in a noSQL environment.  What has not caught on yet in the nosql world is that you CAN do joins with select statements but need to do so in virtual databases(which I will explain below).  The problem with scalability on old RDBMS systems is really the indexing is not broken up.  If you want to scale, you want to be able to grow a table to 1 trillion rows BUT you can't have one index for that table.  You can however have 1 billion indexes and scale just fine.  Within this indexing framework, you can do joins.  Or within these virtual datbase views you can do joins(and sometimes across the virtual views as well)

Basically, it is sort of like having virtual databases.  We explored this concept and succeeded in two spaces already which is why we are developing this solution.  First imagine a simple system of performance accounting and reporting and you have an Account table with Activities in your RDBMS and those activities also have securities.  In this model there are two views of the virtual datbase.

The first one is that we had 1 billion rows in the activity table and 1 million accounts so we created one million indexes.  Let's say we also have a small 50 row table of securityType.  We can get the index we want to query like so

Index index = mgr.getIndex(Activity.class, "/activity/byaccount/"+acc.getAccountId())
Query query = index.getNamedQuery("queryByValue");
query.setParameter("value", value);
query.setMaxResults(100);
query.setPageNumber(0);
List<Activity> activities = query.getResultList();

Notice that this scales just fine BUT leaves us in the old school convenience of RDBMS solutions making RDBMS solutions MUCH MUCH easier to scale to infinite nodes.  That is one virtual view of the data. Another virtual view of the data would be by security like so

Index index = mgr.getIndex(Activity.class, "/activity/bysecurity/"+security.getId())
Query query = index.getNamedQuery("queryByValue");
query.setParameter("value", value);
query.setMaxResults(100);
query.setPageNumber(0);
List<Activity> activities = query.getResultList();

This is another virtual view like looking into an RDBMS.

So what about the denormalization hype in noSQL?  Well, be careful.  I was on one project where one request was taking 80 seconds and by re-normalizing all their data and not repeating so much of it, I brought the query down to 200ms.  Behind the scenes the denormalization was causing 1 megabyte of data to be written on the request which normalization avoided.  Denormalization can be good but partitioning of your indexes and normalizing the data is yet another way to solve similar issues.  Do be careful of having one small table that can be a hotspot though.  Like anything, this is a tool and needs to be used correctly.  Like hibernate, it can be used wrong.

What Joins would look like

(We currently don't support joins but are in the process of adding them)
Taking our previous example of the million indexes we have by acount or the huge amount of indexes we have by security, let's say we have a table with no more than 10,000 rows called ActivityTypeInfo which has a column vendor as well as many other columns.  Let's also say our Activity has a column ActivityTypeInfoId for our join.  Now we could do a join like so with any of those million indexes by account or and of the security indexes like so

IndexInfo info = new IndexInfo(ActivityTypeInfo.class, "/onlyoneactivityTypeIndexSinceTableIsSmall");
//NOTE: mgr.getIndex takes the primary index first!!! and then a varargs of the indexes we need to join with after that
Index index = mgr.getIndex(Activity.class, "/activity/bysecurity/"+security.getId(), info);
Query query = index.getNamedQuery("findWithJoinQuery");
query.setParameter("activityValue", 5); //This is a value in our Activity table
query.setParameter("vendor", "companyX"); //This is a value in our ActivityTypeInfo table
List<Activity> activity = query.getResultList();


Transactions

So, what about transactions.  Well, we firmly believe that "could" exist as well within virtual databases.  At this point, we don't have any support for this but we have support for a full unit of work.  All work you do with the NoSQLEntityManager is cached until you flush much like the hibernate concept(ie. we borrowed it from them of course).  This way, we hope noSQL systems will eventually start supporting a unit of work concept where we can either have all that work written to the proper consistency level or rolled back.  Currently there is no known support for this unit of work, but our design is based so systems that do support it can wire
into our SPI. 

Another Use-Case

Another perfect use-case for virtual databases is you are selling to small businesses so you basically design a table called Small Business and each business has tons of clients so you have a SmallBizClients table.  Those clients then have relationships with other clients or products that the small business has so maybe a smallbizproducts table and clientrelationships tables.  You are now good to go and we work on our partitioning of the indexes to create the virtual databases.  Clearly we would want to partition the SmallBizClients table so we can have trillions of clients and billions of small busineses.  We can then grab the index for that small business and start querying the smallbizclients table for that small business.
