######################################################################
# Usage: get_hadoop_libs.sh ( CDH4 | CDH5 | CDH5-YARN | CDH5-2 | CDH5-2-YARN | HDP2-1 | HDP2-2 | ALL ) #
######################################################################

For ease of use, you can download the latest supported hadoop distribution
libraries by using the provided shell script: get_hadoop_libs.sh

This shell script takes the following command line parameters:
APACHE1
APACHE2
CDH4
CDH5
CDH5-YARN
CDH5-2
CDH5-2-YARN
HDP2-1
HDP2-2
IBM-BI3
ALL

Example:
./get_hadoop_libs.sh CDH5 CDH5-YARN

The previous command will download the required jars to run a ScaleOut hServer
job on both CDH5 and CDH5-YARN. 

If desired, you can manually download the libraries from our website at http://www.scaleoutsoftware.com/support/support-downloads/optional_downloads/
