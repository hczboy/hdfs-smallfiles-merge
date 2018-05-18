Introduction:

    this project aims to implement a way to merge small files in HDFS into one Avro file daily (plus update the Avro file path to ElasticSearch)and remove small files that are have been merged automatically and periodically
     
Pre-requisite:

    1. Hadoop 2.7.3.2.6.3.0-235
    2. Oozie 4.2.0.2.6.3.0-235 

This hadoop small files solution is based on Oozie, Avro technologies.

    1. Oozie facilitates recurring job management, including small files merge, remove small files after 
       they are merged into one/several files.
    2. Avro file is as container file for those small files.
    
How to run it:

    1. modify properties(nameNode, jobTracker) in [projectPath]/src/site/conf/job.properties to accommodate your needs 
    2. run "./gradlew clean zip" under project path and a zip file named "smallfiles-process-1.0.0-SNAPSHOT.zip" would be generated under "[projectPath]/build/distributions"  
    3. upload the smallfiles-process-1.0.0-SNAPSHOT.zip to the machine which Oozie client resides and unzip it
    4. run "hdfs dfs -put oozie oozie" to copy dir "oozie" to hdfs  
    5. run "oozie job --oozie http://hanalytics-ambari-master-scus-3.analytics.azure.local:11000/oozie -config oozie/smallfiles-process/job.properties -doas hdfs -run" to launch the Oozie bundle job(which would manage two coordinator jobs(one for small files merge and the other for small files removal))
    Note: -doas hdfs means job submission as user hdfs, since user "hdfs" has the permission for the paths of the small files
