//    Copyright 2016 Karthik Prasad
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//    
//        http://www.apache.org/licenses/LICENSE-2.0
//    
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.


// Utilities to support list operations in the scheduler.
// Packaged separately as this implementation is not tightly linked to the main component

package org.stark.storm.utils;


import java.util.ArrayList;
import java.util.List;

public class Utils {
    // Zipping Lists in Java: http://stackoverflow.com/a/3843234/3588570
    public static <T> List<List<T>> zip(List<List<T>> lists) {
        List<List<T>> zipped = new ArrayList<List<T>>();
        for (List<T> list : lists) {
            for (int i = 0, listSize = list.size(); i < listSize; i++) {
                List<T> zipList;
                if (i >= zipped.size()) {
                    zipped.add(zipList = new ArrayList<T>());
                }
                else {
                    zipList = zipped.get(i);
                }
                zipList.add(list.get(i));
            }
        }
        return zipped;
    }
    
    // Flattening the list
    public static <T> List<T> flattenList(List<List<T>> listOfLists) {
        List<T> flatList = new ArrayList<T>();
        listOfLists.forEach(flatList::addAll);
        return flatList;
    }
}
