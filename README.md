The generic program calls a HANA Server Side Java Script, which can be called from HADOOP.

see following for example:
http://scn.sap.com/community/developer-center/hana/blog/2014/01/21/creating-a-hana-workflow-using-hadoop-oozie


The HADOOP Java program accepts a minimum of 4 input arguments:
arg[0]  - URL of a HANA XSJS, accessible via the HADOOP cluster
arg[1]  - HANA User name
arg[2]  - HANA Password
arg[3]  - HADOOP HDFS Output directory for storing response
arg[4 to n] - are used for the input parameters for the HANA XSJS called


