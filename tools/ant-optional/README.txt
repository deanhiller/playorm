Why put ant optional tasks here.  Well first read the FAQ at
ant.apache.org http://ant.apache.org/faq.html#delegating-classloader.

I am following option 2.  If I don't, I have to put junit.jar in two places

tools/ant/lib with the ant-junit task
AND
input/lib for application developers to use the junit.jar

Instead, I separate out the ant-junit.jar and put it in the 
tools/ant-optional folder.

Then I taskdef junit against the ant-junit.jar with junit in it's classpath
which I put in

input/lib/tools

If anyone has a better suggestion, I am all ears as this is not the optimal
solution I think.  nonetheless, it works for now.
thanks,
dean