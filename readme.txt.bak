nosqlORM wants to be the first ORM layer with full indexing AND joins in a noSQL environment.  
What has not caught on yet in the nosql world is that you CAN do joins with select statements.  The
problem with scalability on old RDBMS systems is really the indexing is not broken up.  If you want
to scale, you want to be able to grow a table to 1 trillion rows BUT you can't have one index for
that table.  You can however have 1 billion indexes and scale just fine.  Within this indexing
framework, you can do joins.  

Basically, it is sort of like having virtual databases.  We explored this concept and succeeded in
two spaces already which is why we are developing this solutions.  First imagine a simple system
of performance accounting and reporting and you have an Account table with Activities in your RDBMS
and those activities also have securities.  In this model there are two views of the virtual datbase.

The first one is that we had 1 billion rows in the activity table and 1 million accounts so we
created one million indexes.  Let's say we also have a small 50 row table of securityType.  We can
get the index we want to query like so

Index index = mgr.getIndex(Activity.class, "/activity/byaccount/"+acc.getAccountId())
Query query = index.getNamedQuery("queryByValue");
query.setParameter("value", value);
query.setMaxResults(100);
query.setPageNumber(0);
List<Activity> activities = query.getResultList();

Notice that this scales just fine BUT leaves us in the old school convenience of RDBMS solutions making
RDBMS solutions MUCH MUCH easier to scale to infinite nodes.  That is one virtual view of the data.
Another virtual view of the data would be by security like so

Index index = mgr.getIndex(Activity.class, "/activity/bysecurity/"+security.getId())
Query query = index.getNamedQuery("queryByValue");
query.setParameter("value", value);
query.setMaxResults(100);
query.setPageNumber(0);
List<Activity> activities = query.getResultList();

This is another virtual view like looking into an RDBMS.

So what about the denormalization hype in noSQL?  Well, be careful.  I was on one project where one request
was taking 80 seconds and by re-normalizing all their data and not repeating so much of it, I brought
the query down to 200ms.  Behind the scenes the denormalization was causing 1 megabyte of data to be
written on the request which denormalization avoided.  Denormalization can be good but partitioning
of your indexes and not denormalizing the data is yet another way to solve similar issues.


