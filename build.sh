#!/bin/bash -e

# SOSS installation directories -- default values are the default installation locations
SOSS_HOME=${SOSS_HOME:-/usr/local/soss}
SOSS_JNC_DIR=${SOSS_JNC_DIR:-$SOSS_HOME/java_api}
SOSS_HS_LIB_DIR=${SOSS_HS_LIB_DIR:-$SOSS_JNC_DIR/lib}

# Copy the SOSS Java Named Cache JAR (`soss-jnc-5.2.jar`) from SOSS_HOME's Java API subdirectory to the local build environment
cp ${SOSS_JNC_DIR}/soss-jnc-5.2.jar soss_lib
# Copy the third party library JARs from SOSS_HOME's Java API lib directory to the local build environment
cp ${SOSS_JNC_DIR}/lib/*.jar third_party_lib
cp ${SOSS_HS_LIB_DIR}/*.jar third_party_lib

# Call the get_hadoop_libs.sh script to download the specified third_party_lib Hadoop distribution-specific libraries
pushd third_party_lib
. ../build_output/hslib/get_hadoop_libs.sh ALL
popd

# Remove the soss-hserver-5.2-(distribution name).jar from all downloaded and extracted libraries (the build process will replace it)
find third_party_lib -name "soss-hserver-5.2-*.jar" -type f -exec rm -f {} \;

# Build the ScaleOut hServer JAR files
ant clean all

