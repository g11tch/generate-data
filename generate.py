#from kafka.client import KafkaClient #kafka TODO
#from kafka.consumer import SimpleConsumer #kafka TODO
#from kafka.producer import SimpleProducer, KeyedProducer #kafka TODO



import random
import time
import sys
import hadoopy #hadoop TODO
import os
import datetime

def strTimeProp(start, end, format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))

advertiser_list = [
	"adv1",
	"adv2",
	"adv3",
	"adv4",
	"adv5",
	"adv6",
	"adv7",
	"adv8",
	"adv9"
]

pub_list = [
	"pub1",
	"pub2",
	"pub3",
	"pub4",
	"pub5"
]

website_list = [
	"www.google.com",
	"www.facebook.com",
	"techcrunch.com",
	"stackoverflow.com",
	"steam.com"
]

geo_list = [
	"CN",
	"AS",
	"US",
	"IN",
	"ST",
	"RD"
]


def randomDate(start, end, prop):
    return strTimeProp(start, end, '%m/%d/%Y-%I:%M:%S', prop)



#kafka = KafkaClient("localhost:9092") #kafka TODO
#producer = SimpleProducer(kafka) #kafka TODO

arg2 = 0
arg1 = "test"
arg3 = "/"

try:
	arg1 = sys.argv[1]
except Exception:
	arg1 = "test"

try:
	arg2 = int(sys.argv[2])
except Exception:
	arg2 = 1000


try:
	arg3 = sys.argv[3]
except Exception:
	arg3 = "/logs"

hdfs_path = arg3


if not hadoopy.exists(hdfs_path):
	print "does not exist, hence creating directory in hdfs"
	hadoopy.mkdir(hdfs_path)
else:
	print "writing to hdfs"

if not os.path.exists("./logs"):
    os.makedirs("./logs")

ts = time.time()
ts = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
fw = open("./logs"+"/"+"data"+ts,"w")

dataList = []

for i in xrange(arg2):
	string = randomDate("1/1/2010-1:30:00", "1/1/2014-4:50:60",random.random())+" "+pub_list[int(random.random()*10)%len(pub_list)]+" "+advertiser_list[int(random.random()*10)%len(advertiser_list)]+" "+ website_list[int(random.random()*10)%len(website_list)] + " " + geo_list[int(random.random()*10)%len(geo_list)] + " " +str(round(random.random(),4)) + " " + str(int(random.random()*10000))
	if (i+1)%1000 == 0 :
		fw.close()
		ts = time.time()
		ts = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
		fw = open("./logs/"+"data"+ts+str(int(random.random()*10000)),"w")
	print >> fw, string
	#producer.send_messages(arg1, string) #kafka TODO

fw.close()

hadoopy.put("./logs/*", hdfs_path) #hadoop TODO
#kafka.close() #kafka TODO



for the_file in os.listdir("./logs"):
	file_path = os.path.join("./logs", the_file)
	try:
		if os.path.isfile(file_path):
			os.unlink(file_path)
	except Exception, e:
		print e