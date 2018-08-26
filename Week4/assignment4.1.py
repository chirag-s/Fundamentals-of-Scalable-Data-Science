
# coding: utf-8

# # Assignment 4
# 
# Welcome to Assignment 4. This will be the most fun. Now we will prepare data for plotting.
# 
# Just make sure you hit the play button on each cell from top to down. There are three functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook. Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# In[8]:


#Please don't modify this function
def readDataFrameFromCloudant(database):
    cloudantdata=spark.read.load(database, "org.apache.bahir.cloudant")

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# Sampling is one of the most important things when it comes to visualization because often the data set get so huge that you simply
# 
# - can't copy all data to a local Spark driver (Data Science Experience is using a "local" Spark driver)
# - can't throw all data at the plotting library
# 
# Please implement a function which returns a 10% sample of a given data frame:

# In[19]:


def getSample(df,spark):
    x=df.sample(False,0.1)
    return x


# Now we want to create a histogram and boxplot. Please ignore the sampling for now and retur a python list containing all temperature values from the data set

# In[20]:


def getListForHistogramAndBoxPlot(df,spark):
    temp=spark.sql("SELECT temperature from washing where temperature is not null")
    temp1=temp.rdd.map(lambda row:row.temperature).collect()
    return temp1#YOUR CODE GOES HERE


# Finally we want to create a run chart. Please return two lists (encapusalted in a python tuple object) containing temperature and timestamp (ts) ordered by timestamp. Please refere to the following link to learn more about tuples in python: https://www.tutorialspoint.com/python/python_tuples.htm

# In[29]:


#should return a tuple containing the two lists for timestamp and temperature
#please make sure you take only 10% of the data by sampling
#please also ensure that you sample in a way that the timestamp samples and temperature samples correspond (=> call sample on an object still containing both dimensions)
def getListsForRunChart(df,spark):
    temp_ts=spark.sql("SELECT temperature,ts from washing where temperature is not null order by ts asc")
    temp_ts_rdd=temp_ts.rdd.sample(False,0.1).map(lambda row:(row.ts,row.temperature))
    ts=temp_ts_rdd.map(lambda (ts,voltage):ts).collect()
    temperature=temp_ts_rdd.map(lambda (ts,voltage):voltage).collect()
    x=ts,temperature
    return x


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to connect to the cloudant database. Please have a look at the Video "Overview of end-to-end scenario" of Week 2 starting from 6:40 in order to learn how to obtain the credentials for the database. Please paste this credentials as strings into the below code
# 
# ### TODO Please provide your Cloudant credentials here

# In[22]:


hostname = "2ae7b9a4-c7f3-4e8b-95f6-c9ccd849046d-bluemix.cloudant.com"
user = "2ae7b9a4-c7f3-4e8b-95f6-c9ccd849046d-bluemix"
pw = "7e1fc39c17b1bb578cd1646bd2e2a7f2e27a16298b06e735c2a5a2d25a997a38"
database = "washing" #as long as you didn't change this in the NodeRED flow the database name stays the same


# In[23]:


spark = SparkSession    .builder    .appName("Cloudant Spark SQL Example in Python using temp tables")    .config("cloudant.host",hostname)    .config("cloudant.username", user)    .config("cloudant.password",pw)    .getOrCreate()
cloudantdata=readDataFrameFromCloudant(database)


# In[24]:


get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt


# In[25]:


plt.hist(getListForHistogramAndBoxPlot(cloudantdata,spark))
plt.show()


# In[26]:


plt.boxplot(getListForHistogramAndBoxPlot(cloudantdata,spark))
plt.show()


# In[27]:


lists = getListsForRunChart(cloudantdata,spark)


# In[28]:


plt.plot(lists[0],lists[1])
plt.xlabel("time")
plt.ylabel("temperature")
plt.show()


# Congratulations, you are done! Please download the notebook as python file, name it assignment4.1.py and sumbit it to the grader.
