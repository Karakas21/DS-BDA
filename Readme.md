# Business logic:
Program which count average bytes per request by IP and total bytes by IP. 
Output is file with rows as: IP,175.5,109854. For logs like:
99.168.127.53 - - [20/May/2010:07:34:13 +0100] "GET /media/img/m-inact.gif HTTP/1.1" 200 2571 "http://www.example.com/" "Mozilla/5.0 (iPhone; U; CPU iPhone OS 3_1_3 like Mac OS X; en-us) AppleWebKit/528.18 (KHTML, like Gecko) Version/4.0 Mobile/7E18 Safari/528.16"
67.195.114.50 - - [20/May/2010:07:35:27 +0100] "GET /post/261556/ HTTP/1.0" 404 15 "-" "Mozilla/5.0 (compatible; Yahoo! Slurp/3.0; http://help.yahoo.com/help/us/ysearch/slurp)"

# Output Format
Sequence file 

# Additional Requirements
Counters is used for statistics about malformed rows collection


# Mandatory
ZIP-ed src folder with your implementation
Screenshot of successfully executed tests
Screenshot of successfully uploaded file into HDFS 
Screenshots of successfully executed job and result 
Quick build and deploy manual (commands, OS requirements etc)
+ (Optional)
Screenshot with information about memory consumption

