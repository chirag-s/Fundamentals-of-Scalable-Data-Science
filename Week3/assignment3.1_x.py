
# coding: utf-8

# # Assignment 3
# 
# Welcome to Assignment 3. This will be even more fun. Now we will calculate statistical measures on the test data you have created.
# 
# YOU ARE NOT ALLOWED TO USE ANY OTHER 3RD PARTY LIBRARIES LIKE PANDAS. PLEASE ONLY MODIFY CONTENT INSIDE THE FUNCTION SKELETONS
# Please read why: https://www.coursera.org/learn/exploring-visualizing-iot-data/discussions/weeks/3/threads/skjCbNgeEeapeQ5W6suLkA
# . Just make sure you hit the play button on each cell from top to down. There are seven functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
# Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# In[85]:


#Please don't modify this function
def readDataFrameFromCloudant(database):
    cloudantdata=spark.read.load(database, "org.apache.bahir.cloudant")

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# All functions can be implemented using DataFrames, ApacheSparkSQL or RDDs. We are only interested in the result. You are given the reference to the data frame in the "df" parameter and in case you want to use SQL just use the "spark" parameter which is a reference to the global SparkSession object. Finally if you want to use RDDs just use "df.rdd" for obtaining a reference to the underlying RDD object. 
# 
# Let's start with the first function. Please calculate the minimal temperature for the test data set you have created. We've provided a little skeleton for you in case you want to use SQL. You can use this skeleton for all subsequent functions. Everything can be implemented using SQL only if you like.

# In[86]:


def minTemperature(df,spark):
    minRow=df.agg({"temperature": "min"}).collect()[0]
    mintemp = minRow["min(temperature)"]
    print mintemp
    return mintemp


# Please now do the same for the mean of the temperature

# In[87]:


def meanTemperature(df,spark):
    avgRow=df.agg({"temperature": "avg"}).collect()[0]
    avgtemp = avgRow["avg(temperature)"]
    print avgtemp
    return avgtemp


# Please now do the same for the maximum of the temperature

# In[88]:


def maxTemperature(df,spark):
    maxRow=df.agg({"temperature": "max"}).collect()[0]
    maxtemp = maxRow["max(temperature)"]
    print maxtemp
    return maxtemp


# Please now do the same for the standard deviation of the temperature

# In[89]:


def sdTemperature(df,spark):
    temprddrow = df.select('temperature').rdd #in row(temp=x) format
    temprdd = temprddrow.map(lambda (x) : x["temperature"]) #only numbers
    temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "") #remove None params
    n = float(temp.count())
    sum=temp.sum()
    mean =sum/n
    from math import sqrt
    sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
    print sd
    return sd##INSERT YOUR CODE HERE##


# Please now do the same for the skew of the temperature. Since the SQL statement for this is a bit more complicated we've provided a skeleton for you. You have to insert custom code at four position in order to make the function work. Alternatively you can also remove everything and implement if on your own. Note that we are making use of two previously defined functions, so please make sure they are correct. Also note that we are making use of python's string formatting capabilitis where the results of the two function calls to "meanTemperature" and "sdTemperature" are inserted at the "%s" symbols in the SQL string.

# In[90]:


def skewTemperature(df,spark):    
    temprddrow = df.select('temperature').rdd #in row(temp=x) format
    temprdd = temprddrow.map(lambda (x) : x["temperature"]) #only numbers
    temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "")  #remove None params
    n = float(temp.count())
    sum=temp.sum()
    mean =sum/n
    from math import sqrt
    sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
    skew=n*(temp.map(lambda x:pow(x-mean,3)/pow(sd,3)).sum())/(float(n-1)*float(n-2))
    print skew
    return skew 


# Kurtosis is the 4th statistical moment, so if you are smart you can make use of the code for skew which is the 3rd statistical moment. Actually only two things are different.

# In[91]:


def kurtosisTemperature(df,spark): 
    temprddrow = df.select('temperature').rdd
    temprdd = temprddrow.map(lambda (x) : x["temperature"])
    temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "")
    n = float(temp.count())
    sum=temp.sum()
    mean =sum/n
    from math import sqrt
    sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
    kurtosis=temp.map(lambda x:pow(x-mean,4)).sum()/(pow(sd,4)*(n))
    print kurtosis
    return kurtosis


# Just a hint. This can be solved easily using SQL as well, but as shown in the lecture also using RDDs.

# In[92]:


def correlationTemperatureHardness(df,spark):
    temprddrow = df.select('temperature').rdd
    temprdd = temprddrow.map(lambda (x) : x["temperature"])
    temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "")
    hardrddrow = cloudantdata.select('hardness').rdd
    hardrdd = hardrddrow.map(lambda (x) : x["hardness"])
    hard = hardrdd.filter(lambda x: x is not None).filter(lambda x: x != "")
    n = float(temp.count())
    sumt=temp.sum()
    sumh=hard.sum()
    meant =sumt/n
    meanh = sumh/n
    rdd12 = temp.zip(hard)
    cov12 = rdd12.map(lambda (x,y) : (x-meant)*(y-meanh)).sum()/n
    from math import sqrt
    sd1 = sqrt(temp.map(lambda x : pow(x-meant,2)).sum()/n)
    sd2 = sqrt(hard.map(lambda x : pow(x-meanh,2)).sum()/n)
    corr12 = cov12 / (sd1 * sd2)
    corr12
    return corr12


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to connect to the cloudant database. Please have a look at the Video "Overview of end-to-end scenario" of Week 2 starting from 6:40 in order to learn how to obtain the credentials for the database. Please paste this credentials as strings into the below code
# 
# ### TODO Please provide your Cloudant credentials here

# In[93]:


hostname = "2ae7b9a4-c7f3-4e8b-95f6-c9ccd849046d-bluemix.cloudant.com"
user = "2ae7b9a4-c7f3-4e8b-95f6-c9ccd849046d-bluemix"
pw = "7e1fc39c17b1bb578cd1646bd2e2a7f2e27a16298b06e735c2a5a2d25a997a38"
database = "washing" #as long as you didn't change this in the NodeRED flow the database name stays the same


# In[94]:


spark = SparkSession    .builder    .appName("Cloudant Spark SQL Example in Python using temp tables")    .config("cloudant.host",hostname)    .config("cloudant.username", user)    .config("cloudant.password",pw)    .getOrCreate()
cloudantdata=readDataFrameFromCloudant(database)


# In[95]:


minTemperature(cloudantdata,spark)
#x1=cloudantdata.agg({"temperature": "min"}).collect()[0]
#print x1
#print x1["min(temperature)"]


# In[96]:


meanTemperature(cloudantdata,spark)


# In[97]:


maxTemperature(cloudantdata,spark)


# In[98]:


sdTemperature(cloudantdata,spark)
#temprddrow = cloudantdata.select('temperature').rdd
#temprdd = temprddrow.map(lambda (x) : x["temperature"])
#temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "")
#n = float(temp.count())
#sum=temp.sum()
#mean =sum/n
#from math import sqrt
#sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)

#fd2=fd2.take(n)
#fd2 = filter(None, fd2)
#[x for x in fd2 if x is not None]
#print n
#print sum
#print mean
#print sd


# In[99]:


skewTemperature(cloudantdata,spark)
#temprddrow = cloudantdata.select('temperature').rdd
#temprdd = temprddrow.map(lambda (x) : x["temperature"])
#temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "")
#n = float(temp.count())
#sum=temp.sum()
#mean =sum/n
#from math import sqrt
#sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
#skew=n*(temp.map(lambda x:pow(x-mean,3)/pow(sd,3)).sum())/(float(n-1)*float(n-2))
#print skew


# In[100]:


kurtosisTemperature(cloudantdata,spark)
#temprddrow = cloudantdata.select('temperature').rdd
#temprdd = temprddrow.map(lambda (x) : x["temperature"])
#temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "")
#n = float(temp.count())
#sum=temp.sum()
#mean =sum/n
#from math import sqrt
#sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
#kurtosis=temp.map(lambda x:pow(x-mean,4)).sum()/(pow(sd,4)*(n))
#print kurtosis


# In[101]:


correlationTemperatureHardness(cloudantdata,spark)
#temprddrow = cloudantdata.select('temperature').rdd
#temprdd = temprddrow.map(lambda (x) : x["temperature"])
#temp = temprdd.filter(lambda x: x is not None).filter(lambda x: x != "")
#hardrddrow = cloudantdata.select('hardness').rdd
#hardrdd = hardrddrow.map(lambda (x) : x["hardness"])
#hard = hardrdd.filter(lambda x: x is not None).filter(lambda x: x != "")
#n = float(temp.count())
#sumt=temp.sum()
#sumh=hard.sum()
#meant =sumt/n
#meanh = sumh/n
#rdd12 = temp.zip(hard)
#cov12 = rdd12.map(lambda (x,y) : (x-meant)*(y-meanh)).sum()/n
#from math import sqrt
#sd1 = sqrt(temp.map(lambda x : pow(x-meant,2)).sum()/n)
#sd2 = sqrt(hard.map(lambda x : pow(x-meanh,2)).sum()/n)
#corr12 = cov12 / (sd1 * sd2)
#corr12
#from math import sqrt
#sd=sqrt(temp.map(lambda x : pow(x-mean,2)).sum()/n)
#kurtosis=temp.map(lambda x:pow(x-mean,4)).sum()/(pow(sd,4)*(n))
#print kurtosis""""


# Congratulations, you are done, please download this notebook as python file using the export function and submit is to the gader using the filename "assignment3.1.py"
