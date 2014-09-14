****runnig script to generate data****

**this will generate 1000 record log data, inside /logs hdfs directory**

  $python generate.py

**this will generate 10000 record log data, inside /logs hdfs directory**

  $python generate.py test 10000

**this will generate 10000 record log data, inside /log_data hdfs directory**
  $python generate.py test 10000 /log_data


***data formate timestamp, publisher, advertiser, website, country code, bid price and cookie id***

03/13/2013-02:01:11 pub3 adv8 www.google.com US 0.298689199203 480 <br/>
01/17/2010-10:15:33 pub4 adv1 techcrunch.com AS 0.267062313494 5479 <br/>
08/07/2011-02:51:35 pub5 adv3 stackoverflow.com RD 0.343185186326 6003 <br/>
02/21/2010-03:24:45 pub1 adv8 techcrunch.com RD 0.0729840643449 8008 <br/>
11/03/2013-06:42:00 pub5 adv2 techcrunch.com CN 0.895264369496 492 <br/>
11/18/2011-07:29:29 pub1 adv6 stackoverflow.com US 0.414414584931 2559 <br/>
08/02/2010-10:31:47 pub5 adv2 techcrunch.com US 0.100531073119 9782 <br/>
04/13/2011-11:47:15 pub4 adv3 www.google.com US 0.342579317704 6078 <br/>
