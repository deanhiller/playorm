## Support

``` We answer questions on stackoverflow, so just tag the question with "playOrm".```

For paid support send an email to dean at alvazan.com. We support clients in Asia, Europe, South and North America.

For training in the US, feel free to contact us as well.

***Developers***: Please help us by encouraging those people with the money to utilize our support and/or training as we use the money to create a better product for you and the more we can split the cost between many companies, the cheaper it is to add more and more features.  Also, please write a blog or link to us...we can always use more marketing and don't forget to star our github project ;).  Even in open source, marketing helps the project become much better for your use.

Recently, We are working more and more on matching any model you throw at us.  We want to work with the part of your data that is structured and allow you to still have tons of unstructured data.

MAIN FEATURE: We are a Partial Schema or Partially Structured data ORM meaning not all of your schema has to be defined!!!!  We love the schemaless concept of noSQL and want to embrace it, but when it comes time to make your reads faster, we index the writes you want us to on your behalf so that your reads can be even faster so we are partially structured because we need to know what to index ;) .

We also embrace embedding information in rows so you can do quick one key lookups unlike a JPA would let you do.  On top of that, we still support relations between these datasets as well allowing you to do joins.  

## PlayOrm Feature List

* [@NoSqlEmbedded for List&lt;Integer&gt;, List&lt;LocaDate&gt;, List&lt;String&gt;,](https://github.com/deanhiller/playorm/wiki/@NoSqlEmbedded-for-Integer,-LocalDate-and-String-list) etc. etc. (something NOT in JPA)
* In Playorm, [Entity can have a Cursor instead of List](https://github.com/deanhiller/playorm/wiki/Support-for-cursor-in-an-Entity-for-very-wide-rows) which is lazy read to prevent out of memory on VERY wide rows
* [PlayOrm Queries use way less resources from cassandra cluster than CQL queries](https://github.com/deanhiller/playorm/wiki/Fast-Scalable-Queries)
* [Scalabla JQL(SJQL)](https://github.com/deanhiller/playorm#virtual-databases-and-index-partitioning) supported which is modified JQL that scales(SQL doesn't scale well)
* [Partitioning](https://github.com/deanhiller/playorm#virtual-databases-and-index-partitioning) so you can query a one trillion row table in just ms with SJQL(Scalable Java Query Language)
* Typical query support of [ <=, <, >, >= and = and no limitations here] (https://github.com/deanhiller/playorm/wiki/SJQL-Support)
* Typical query support of [AND and OR as well as parenthesis ] (https://github.com/deanhiller/playorm/wiki/SJQL-Support)
* [Inner Join and Left Outer Join support](https://github.com/deanhiller/playorm#now-joins) (Must keep your very very large tables partitioned so you get very fast access times here)
* Return Database cursor on query. [See an example how it works] (https://github.com/deanhiller/playorm/wiki/An-example-to-begin-with-PlayOrm)
* [OneToMany, ManyToMany] (https://github.com/deanhiller/playorm/wiki/A-basic-*ToMany-example-TestOneToMany), [OneToOne, and ManyToOne] (https://github.com/deanhiller/playorm/wiki/A-basic-*ToOne-example) but the ToMany's are nosql fashion not like RDBMS
* [Support of a findAll(Class c, List<Object> keys)](https://github.com/deanhiller/playorm/wiki/Support-for-retrieving-many-entities-in-parallel) as is typical in nosql to parallel the reads
* [Inheritance class heirarchy in one table] (https://github.com/deanhiller/playorm/wiki/A-basic-inheritance-example) is supported like hibernate
* [flush() support](https://github.com/deanhiller/playorm#flush) - We protect you from failures!!!
* [First level read cache] (https://github.com/deanhiller/playorm/wiki/Caching-in-Playorm)
* Automatically creates ColumnFamilies at runtime. [Check this example] (https://github.com/deanhiller/playorm/wiki/Create-your-first-Entity) to know how easy it is to create an entity using Playorm
* Includes it's own [in-memory database for TDD] (https://github.com/deanhiller/playorm#note-on-test-driven-development) in your unit tests!!!!!
* [Saves you MORE data storage compared to other solutions](https://github.com/deanhiller/playorm/wiki/An-important-note-on-storage)
* [logging interface] (https://github.com/deanhiller/playorm/wiki/Logging-to-know-for-your-benefit) below the first level cache so you can see the raw operations on cassandra and optimize just like when you use hibernate's logging
* Log all your webservers logs to a fixed number of rows in cassandra so you can have one view into your webserver logs
* [A raw interface] (https://github.com/deanhiller/playorm/wiki/An-important-note-on-storage#The-Raw-Typed-Interface) using only BigDecimal, BigInteger, and String types which is currently used to upload user defined datasets through a web interface(and we wire that into generating meta data so they can ad-hoc query on the nosql system)
* [An ad-hoc query interface] (https://github.com/deanhiller/playorm/wiki/A-basic-ad-hoc-query-example) that can query on any table that was from an Entity object.  To us on other tables, you can also code up and save DboTableMeta objects and the ad-hoc query interface gets you query support into those tables
* If you have some noSQL data and some Relational data, use Playorm and store your relational data in noSQL now and just maintain one database in production! As Playorm supports [SQL] (https://github.com/deanhiller/playorm/wiki/SJQL-Support), and many relations like [*ToOne] (https://github.com/deanhiller/playorm/wiki/A-basic-*ToOne-example) and [*ToMany] (https://github.com/deanhiller/playorm/wiki/A-basic-*ToMany-example-TestOneToMany)
* [Support for joda-time LocalDateTime, LocalDate, LocalTime] (https://github.com/deanhiller/playorm/wiki/Date-and-Calendar-Support) which works way better than java's Date object and is less buggy than java's Date and Calendar objects
* [Command Line tool] (https://github.com/deanhiller/playorm/wiki/Command-Line-Tool)
* [A plugin for PlayFramework 1.2.x](https://github.com/deanhiller/playorm/wiki/PlayFramework-1.2.x-Support)
* [Support for all major data types] (https://github.com/deanhiller/playorm/wiki/Data-types-supported) with an option to [create your own custom converter] (https://github.com/deanhiller/playorm/wiki/Writing-a-data-type-converter)


### Features soon to be added
* WAY better support for @Embedded and putting Map<String, Type> in your entities for wide row support
* Ability to index fields inside Embedded objects even if embedded object is a list so you can query them still
* TONS of documentation is in the works due out.  Check wiki for latest dates
* More work around the command line tool 
* Map/Reduce tasks for re-indexing, or creating new indexes from existing data
* MANY MANY optimizations can be made to increase performance like a nested lookahead loop join and other tricks that only work in noSQL
* We are considering a stateless server that exposes JDBC BUT requires S-SQL commands (OR just SQL commands for non-partitioned tables)


### Flush
A major problem with people using Hector and Astyanax is they do not queue up all their writes/mutations to be done at the end of processing so if something fails in the middle, ONLY HALF of the data is written and you end up with a corrupt noSql database.  The flush method on PlayOrm is what pushes all your persists down in one shot so it sort of sends it as a unit of work(NOT a transaction).  If there is an exception before the flush, nothing gets written to the nosql store.

### Note on Test Driven Development

We believe TDD to be very important and believe in more component testing than Functional or QA testing(testing that includes testing more code than unit tests typically do, but less code then a full QA test so we can simulate errors that QA cannot).  For this reason, the first priority of this project is to always have an in-memory implementation of indexes, databases, etc. so that you can use a live database during your unit testing that is in-memory and easily wipe it by just creating another one.

### Virtual Databases and Index Partitioning

If a DBMS could be on multiple nodes, what is the one major issue with scalability for the DBMS?  Think about it.  As a table reaches 1 trillion rows, what is the issue?  The issue is your index size has grown to a point that inserts and removes and queries take too long.  We want SMALLER indices.  This is where partitioning comes in.  Most OLTP shops have many customers and want to keep adding customers.  That customer id is a great way to partition your data and still be able to do joins in what we call Scalable SQL or Scalable JQL

We explored this concept and succeeded in two spaces already which is why we are developing this solution(this solution is live in production with our first client as well).  First imagine a simple system of performance accounting and reporting and you have an Account table with Activities in your noSql store.  Now, let's say we had 1 billion rows in the activity table and 100k accounts and we decide to partition our activity table by the account.  This means we end up with 100k partitions(This means on average 10000 activities in each partition ).  With that design in mind, let's code....

```
//First, on the Activity.java, you will have a NoSqlQuery like so
@NoSqlQuery(name="queryByValue", query="PARTITIONS a(:partId) SELECT a FROM TABLE as a "+
                        "WHERE a.price > :value and a.numShares < 10")

//Next, on one of the fields of Activity.java, you will have a field you use to partition by like so

   @NoSqlPartitionByThisField
   @ManyToOne
   private Account account; //NOTE: This can be primitive or whatever you like

//After, that, just save your entities, remove your entites, we do the 
//heavy lifting of adding and removing from indexes(when you update, we remove 
//the old value from the index AND save the new value to the index

   entityMgr.put(activity1); //automatically saved to it's partition
   entityMgr.put(activity2); //automatically saved to it's partition

//NEXT, we want to query so we setParameter and give it the partition id
   Account partitionId = someAccount;
   Query query = entityManager.createNamedQuery("queryByValue");
   query.setParameter("value", value);
   query.setParameter("partId", partitionId);
   query.setMaxResults(100);
   query.setPageNumber(0);
   List<Activity> activities = query.getResultList();
```
Notice that this scales just fine BUT leaves us in the old school convenience of RDBMS solutions making RDBMS solutions MUCH MUCH easier to scale to infinite nodes.  That is one virtual view of the data.  You can partition by more than one field but we leave that for later tutorials.

So what about the denormalization hype in noSQL?  Well, be careful.  I was on one project where one request was taking 80 seconds and by re-normalizing all their data and not repeating so much of it, I brought the query down to 200ms.  Behind the scenes the denormalization was causing 1 megabyte of data to be written on the request which normalization avoided.  Denormalization can be good but partitioning of your indexes and normalizing the data is yet another way to solve similar issues.  I do NOT encourage you to always normalize, just sometimes.  Do be careful of having one small table that can be a hotspot though.  Like anything, this is a tool and needs to be used correctly.  Like hibernate, it can be used wrong.

### Now, Joins

**INNER JOIN**

Taking our previous example of the 100k partitions, let's say we have a table with no more than 10,000 rows called ActivityTypeInfo which has a column vendor as well as many other columns.  Let's also say our Activity has a column ActivityTypeInfoId for our join.  Now we could do a join like so with any of those 100k partitions

```
//First, our NoSqlQuery again that would be on our Activity.java class

@NoSqlQuery(name="findWithJoinQuery", query="PARTITIONS t(:partId) SELECT t FROM TABLE as t "+
"INNER JOIN t.activityTypeInfo as i WHERE i.type = :type and t.numShares < :shares"),

//NOW, we run the simple query
Query query = entityMgr.getNamedQuery("findWithJoinQuery");
query.setParameter("type", 5); 
query.setParameter("shares", 28); 
query.setParameter("partId", null);  //Here we are saying to use the 'null' partition
                                     //Where any activities with no account will end up
List<Activity> activity = query.getResultList();
```

**LEFT OUTER JOIN**

In continuation of above code, we can have NoSqlQuery again that would be on our Activity.java class


```

@NoSqlQuery(name="findWithJoinQuery", query="PARTITIONS t(:partId) SELECT t FROM TABLE as t "+
"LEFT JOIN t.activityTypeInfo as i WHERE i.type = :type and t.numShares < :shares"),

//NOW, we run the simple query
Query query = entityMgr.getNamedQuery("findWithJoinQuery");
query.setParameter("type", 5); 
query.setParameter("shares", 28); 
query.setParameter("partId", null);  //Here we are saying to use the 'null' partition
                                     //Where any activities with no account will end up
List<Activity> activity = query.getResultList();
```


## Another Use-Case

Another perfect use-case for virtual databases is you are selling to small businesses so you basically design a table called Small Business and each business has tons of clients so you have a SmallBizClients table.  Those clients then have relationships with other clients or products that the small business has so maybe a smallbizproducts table and clientrelationships tables.  You are now good to go and we work on our partitioning of the indexes to create the virtual databases.  Clearly we would want to partition the SmallBizClients table so we can have trillions of clients and billions of small busineses.  We can then grab the index for that small business and start querying the smallbizclients table for that small business.

## TODO
* adding JDBC such legacy apps can work with non-partitioned tables AND other apps can be modified to prefix queries with the partition ids and still use JDBC so they can just make minor changes to their applications(ie. keep track of partition ids)!!!  NOTE: This is especially useful for reporting tools
