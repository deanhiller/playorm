***While the source code will remain on github, the feature list and documentation has moved to [Buffalosw.com](http://buffalosw.com/products/playorm/)***

## Support

***NOTE*** PlayOrm is really PlayONM or object to NoSql mapping as it is more one to one with NoSQL than JPA is utilizing many of the wide row and
other NoSql patterns on your behalf.  Otherwise, those patterns are quite a bit of work.

``` We answer questions on stackoverflow, so just tag the question with "playOrm".```

For paid support send an email to dean at alvazan.com. We support clients in Asia, Europe, South and North America.

For training in the US, feel free to contact us as well.

***Developers***: Please help us by encouraging those people with the money to utilize our support and/or training as we use the money to create a better product for you and the more we can split the cost between many companies, the cheaper it is to add more and more features.  Also, please write a blog or link to us...we can always use more marketing and don't forget to star our github project ;).  Even in open source, marketing helps the project become much better for your use.

Recently, We are working more and more on matching any model you throw at us.  We want to work with the part of your data that is structured and allow you to still have tons of unstructured data.

MAIN FEATURE: We are a Partial Schema or Partially Structured data ORM meaning not all of your schema has to be defined!!!!  We love the schemaless concept of noSQL and want to embrace it, but when it comes time to make your reads faster, we index the writes you want us to on your behalf so that your reads can be even faster so we are partially structured because we need to know what to index ;) .

We also embrace embedding information in rows so you can do quick one key lookups unlike a JPA would let you do.  On top of that, we still support relations between these datasets as well allowing you to do joins.  

## PlayOrm Feature List
While the source code will remain on github, the feature list and documentation has moved to [Buffalosw.com](http://buffalosw.com/products/playorm/)


### Features soon to be added
* Ability to index fields inside Embedded objects even if embedded object is a list so you can query them still
* Map/Reduce tasks for re-indexing, or creating new indexes from existing data
* MANY MANY optimizations can be made to increase performance like a nested lookahead loop join and other tricks that only work in noSQL
* We are considering a stateless server that exposes JDBC BUT requires S-SQL commands (OR just SQL commands for non-partitioned tables)
* Adding JDBC such legacy apps can work with non-partitioned tables AND other apps can be modified to prefix queries with the partition ids and still use JDBC so they can just make minor changes to their applications(ie. keep track of partition ids)!!!  NOTE: This is especially useful for reporting tools
