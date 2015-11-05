Overview
========
ScaleOut hServer® is a MapReduce execution engine that runs on the [ScaleOut StateServer](http://www.scaleoutsoftware.com/products/scaleout-stateserver/ "ScaleOut StateServer Product Page") (SOSS) in-memory compute engine to deliver fast execution times with linearly scalable speed-up for standard, unchanged Hadoop MapReduce applications. Additionally, the hServer library enables Hadoop MapReduce applications to continuously analyze live, fast-changing, in-memory data as well as HDFS files. A single line code change is all that is required to run a standard Apache Hadoop MapReduce application with ScaleOut hServer without YARN support. If ScaleOut hServer is co-located on a YARN-enabled Hadoop cluster, you can simply set the "mapreduce.framework.name" property to "hserver-yarn" (e.g., adding the `-Dmapreduce.framework.name=hserver-yarn` argument from the command-line) to run a Hadoop MapReduce application unchanged with ScaleOut hServer. For more information regarding ScaleOut hServer, please visit the [ScaleOut hServer product page](http://www.scaleoutsoftware.com/products/scaleout-hserver/ "ScaleOut hServer Product Page") which explains ScaleOut hServer's architecture and usage in detail.

Documentation
=============
The Javadoc for ScaleOut hServer can be found on our website under '[ScaleOut hServer V2 Documentation](http://www.scaleoutsoftware.com/support/stateServer/soss-hserver-5.3-javadoc/index.html)'. The `soss-jnc-5.3-javadoc.jar` JAR is also located on our website under '[API Documentation for ScaleOut StateServer and ScaleOut Analytics Server](http://www.scaleoutsoftware.com/support/stateServer/soss-jnc-5.3-javadoc/index.html)' and provides documentation for SOSS API usage. All additional ScaleOut hServer and ScaleOut StateServer documentation can be found on the ScaleOut Software Support website under [Product Documentation](http://www.scaleoutsoftware.com/support/product-documentation/ "ScaleOut Software Online Documentation").

Building
========

Prerequisites
-------------
1. Download [ScaleOut StateServer](http://www.scaleoutsoftware.com/support/support-downloads/)
2. Install ScaleOut StateServer using hServer's [quick start guide](http://www.scaleoutsoftware.com/support/stateServer/hserver_programmers_guide/content/index.html)
3. [Download](http://ant.apache.org/bindownload.cgi) and [Install](http://ant.apache.org/manual/install.html) Apache Ant

Linux
-----
- Clone the ScaleOut hServer project to a local directory.
- Copy the SOSS Java Named Cache JAR (`soss-jnc-5.3.jar`) from the Linux installation Java API directory, (typically `/usr/local/soss/java_api/`) to the `soss_lib` subdirectory of the ScaleOut hServer source location.
- Copy the third party library JARs from the Linux installation Java API lib directories, (typically `/usr/local/soss/java_api/lib` and `/usr/local/soss/java_api/hslib`) to the `third_party_lib`  subdirectory of the ScaleOut hServer source location.
- Download and copy the third party Hadoop distribution library JARs from our website under [Optional Downloads](http://www.scaleoutsoftware.com/support/support-downloads/optional-downloads/) to the appropriate subdirectory of the third_party_lib subdirectory of the ScaleOut hServer source location. For example, download the Apache Hadoop 2.4.1 JARs and copy them to the "/path/to/source/third_party_lib/hadoop-2.4.1/" directory.

Windows
-------
- Clone the ScaleOut hServer project to a local directory.
- Copy the SOSS Java Named Cache JAR (`soss-jnc-5.3.jar`) from the Windows installation Java API directory, (typically `C:\Program Files\ScaleOut_Software\StateServer\JavaAPI`) to the `soss_lib` subdirectory of the ScaleOut hServer source location.
- Copy the third party library JARs from the Windows installation Java API lib directory, (typically `C:\Program Files\ScaleOut_Software\StateServer\JavaAPI\lib` and `C:\Program Files\ScaleOut_Software\StateServer\JavaAPI\hslib`) to the `third_party_lib` subdirectory of the ScaleOut hServer source location.
- Download and copy the third party Hadoop distribution library JARs from our website under [Optional Downloads](http://www.scaleoutsoftware.com/support/support-downloads/optional-downloads/) to the appropriate subdirectory of the third_party_lib subdirectory of the ScaleOut hServer source location. For example, download the Apache Hadoop 2.4.1 JARs and copy them to the "C:\path\to\source\third_party_lib\hadoop-2.4.1\" directory.


To build all ScaleOut hServer JARs from source
----------------------------------------------
Open a terminal or command prompt inside the hServer base directory and run the following command:
`ant all`

All hServer JARs will be located in the `build_output` directory.

Supported Hadoop Distributions
------------------------------

To build the ScaleOut hServer libraries compiled against a specific supported Hadoop distribution (for example, Apache Hadoop version 2.4.1): 
`ant build-hadoop-2.4.1`

The following Hadoop distributions are currently supported:

|  Distribution Name                                |  Build Target        |  Output Subdirectory  |
|  -----------------                                |  ------------        |  -------------------  |
| Cloudera CDH 4.4.0                                | build-cdh4.4.0       |  cdh4.4.0             |
| Cloudera CDH 5.0.2                                | build-cdh5.0.2       |  cdh5.0.2             |
| Cloudera CDH 5.0.2 (with YARN support)            | build-cdh5.0.2-yarn  |  cdh5.0.2-yarn        |
| Cloudera CDH 5.2.1                                | build-cdh5.2.1       |  cdh5.2.1             |
| Cloudera CDH 5.2.1 (with YARN support)            | build-cdh5.2.1-yarn  |  cdh5.2.1-yarn        |
| Apache Hadoop 1.2.1                               | build-hadoop-1.2.1   |  hadoop-1.2.1         |
| Apache Hadoop 2.4.1                               | build-hadoop-2.4.1   |  hadoop-2.4.1         |
| Hortonworks Data Platform 2.1 (with YARN support) | build-hdp2.1-yarn    |  hdp2.1-yarn          |
| Hortonworks Data Platform 2.2 (with YARN support) | build-hdp2.2-yarn    |  hdp2.2-yarn          |
| IBM BigInsights 3.0.0                             | build-ibm-bi-3.0.0   |  ibm-bi-3.0.0         |

Getting Help 
============
Need help building or deploying ScaleOut hServer? Post your questions on our [forum](https://forum.scaleoutsoftware.com/). 

