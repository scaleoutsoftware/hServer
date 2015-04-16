###########################################################################
# Copyright (c) 2015 by ScaleOut Software, Inc.                           # 
#                                                                         #
# Licensed under the Apache License, Version 2.0 (the "License");         #
# you may not use this file except in compliance with the License.        #
# You may obtain a copy of the License at                                 #
#                                                                         #
# http://www.apache.org/licenses/LICENSE-2.0                              # 
#                                                                         #
# Unless required by applicable law or agreed to in writing, software     #
# distributed under the License is distributed on an "AS IS" BASIS,       # 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.#
# See the License for the specific language governing permissions and     #
# limitations under the License.                                          #
###########################################################################

#!/bin/bash

DISTROS=()

if [ "$1" == "-h" -o "$1" == "--help" ]; then
    cat README.txt
    exit 1;
fi

while (( "$#" )); do

    if [ "$1" == "APACHE1" -o "$1" == "ALL" ]; then
        DISTROS+=('hadoop-1.2.1.tar.gz')
    fi
    
    if [ "$1" == "APACHE2" -o "$1" == "ALL" ]; then
        DISTROS+=('hadoop-2.4.1.tar.gz')
    fi
    
    if [ "$1" == "CDH4" -o "$1" == "ALL" ]; then
        DISTROS+=('cdh4.4.0.tar.gz')
    fi

    if [ "$1" == "CDH5" -o "$1" == "ALL" ]; then
        DISTROS+=('cdh5.0.2.tar.gz')
    fi

    if [ "$1" == "CDH5-YARN" -o "$1" == "ALL" ]; then
        DISTROS+=('cdh5.0.2-yarn.tar.gz')
    fi
    
    if [ "$1" == "CDH5-2" -o "$1" == "ALL" ]; then
        DISTROS+=('cdh5.2.1.tar.gz')
    fi
    
    if [ "$1" == "CDH5-2-YARN" -o "$1" == "ALL" ]; then
        DISTROS+=('cdh5.2.1-yarn.tar.gz')
    fi

    if [ "$1" == "HDP2-1" -o "$1" == "ALL" ]; then
        DISTROS+=('hdp2.1-yarn.tar.gz')
    fi
    
    if [ "$1" == "HDP2-2" -o "$1" == "ALL" ]; then
        DISTROS+=('hdp2.2-yarn.tar.gz')
    fi
    
    if [ "$1" == "IBM-BI3" -o "$1" == "ALL" ]; then
        DISTROS+=('ibm-bi-3.0.0.tar.gz')
    fi
        
    shift

done

for DISTRO in "${DISTROS[@]}"; do
    curl http://www.scaleoutsoftware.com/pkg/${DISTRO} | tar xvz
done

