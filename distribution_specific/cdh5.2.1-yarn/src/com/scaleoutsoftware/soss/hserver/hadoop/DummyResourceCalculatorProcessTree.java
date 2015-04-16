/*
 Copyright (c) 2015 by ScaleOut Software, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.scaleoutsoftware.soss.hserver.hadoop;


import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

/**
 * This class stubs out  ResourceCalculatorProcessTree, which is used by
 * task initialization.
 */
public class DummyResourceCalculatorProcessTree extends ResourceCalculatorProcessTree {

    public DummyResourceCalculatorProcessTree(String root) {
        super(root);
    }

    @Override
    public void updateProcessTree() {

    }

    @Override
    public String getProcessTreeDump() {
        return "";
    }

    @Override
    public long getCumulativeVmem(int i) {
        return 0;
    }

    @Override
    public long getCumulativeRssmem(int i) {
        return 0;
    }

    @Override
    public long getCumulativeCpuTime() {
        return 0;
    }

    @Override
    public boolean checkPidPgrpidForMatch() {
        return false;
    }
}
