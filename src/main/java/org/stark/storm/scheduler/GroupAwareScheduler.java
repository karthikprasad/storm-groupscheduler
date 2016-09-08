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

// The storm scheduler is responsible for assignment of of the components (bolts and spouts)
// to the processes and the machines they run on. A scheduler works in two phases
//   i) it first assigns the components (each tied to an executor) to workers (JVM processes)
//   ii) and then assigns these workers to the supervisors (nodes)
// The default storm scheduler follows round-robin strategy during each of these assignments
// This, typically, helps balance out the resource usage; but not always.
// When the tuples emitted by either a spout or a bolt is too big, and the participating
// components are on different physical machines, network latency increases thus decreasing
// throughput.
// 
// This scheduler assigns the components of the same group to the same processes. They are
// only  grouped logically and not tied to a particular machine. The default scheduler takes
// up the job of assigning processes to machines.
// The components can be added to the same group by specifying the same group ID via the
// addConfiguration() while defining the topology. (See example topology)

package org.stark.storm.scheduler;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.EvenScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.shade.com.google.common.collect.ArrayListMultimap;
import org.apache.storm.shade.com.google.common.collect.ListMultimap;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;


import org.stark.storm.utils.Utils;


public class GroupAwareScheduler implements IScheduler {
    @Override
    public void prepare(Map conf) {}

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        System.out.println("========================================================================");
        System.out.println("GroupAwareScheduler: begin scheduling");
        // Gets the topology which we want to schedule
        // The custom scheduler is run only on the specified topology, while using the default scheduler for others
        TopologyDetails topology = topologies.getByName("JARVIS"); // CONFIGURABLE PARAMETER

        // make sure the our topology is submitted,
        if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);

            if (needsScheduling) {
                // 0. print out info
                System.out.println("Max Num workers per topology: " + topology.getNumWorkers());
                System.out.println("Assigned Num Workers: " + cluster.getAssignedNumWorkers(topology));
                Map<String, SupervisorDetails> supervisorsMap = cluster.getSupervisors();
                System.out.println("Supervisors: " + supervisorsMap);
                System.out.println("-------------------------------------------");
                for (String supervisor : supervisorsMap.keySet()) {
                    System.out.println("Supervisor " + supervisor);
                    System.out.println("Assignable worker ports: " + cluster.getAssignablePorts(supervisorsMap.get(supervisor)));
                    System.out.println("Assignable worker slots: " + cluster.getAssignableSlots(supervisorsMap.get(supervisor)));
                    System.out.println("Available worker ports: " + cluster.getAvailablePorts(supervisorsMap.get(supervisor)));
                    System.out.println("Available worker slots: " + cluster.getAvailableSlots(supervisorsMap.get(supervisor)));
                    System.out.println("-------------------------------------------");
                }
                
                // 1. get the component -> list of executors running it (already populated)
                Map<String, List<ExecutorDetails>> componentToExecutorsMap = cluster.getNeedsSchedulingComponentToExecutors(topology);
                System.out.println("needs scheduling(component->executor): " + componentToExecutorsMap);
                System.out.println("needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));

                // 2. get a list of workers that we can assign executors to
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
                if (currentAssignment != null) {
                    System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                    System.out.println("current assignments: {}");
                }
                
                // 3. get the component groupings from config
                StormTopology strmTop = topology.getTopology();
                Map<String, SpoutSpec> spouts = strmTop.get_spouts();
                Map<String, Bolt> bolts = strmTop.get_bolts();
                ListMultimap<String, String> groups = ArrayListMultimap.create();
                try {
                    JSONParser parser = new JSONParser();
                    // Iterate over Spouts
                    for (String name : spouts.keySet()) {
                        SpoutSpec spout = spouts.get(name);
                        JSONObject conf = (JSONObject)parser.parse(spout.get_common().get_json_conf());
                        String groupName = (String) conf.get("group");
                        if (groupName != null) {
                            groups.put(groupName, name);
                        }
                    }
                    // Iterate over Bolts
                    for (String name : bolts.keySet()) {
                        Bolt bolt = bolts.get(name);
                        JSONObject conf = (JSONObject)parser.parse(bolt.get_common().get_json_conf());
                        String groupName = (String) conf.get("group");
                        if (groupName != null) {
                            groups.put(groupName, name);
                        }
                    }
                }
                catch (ParseException e) {
                    System.out.println("Exception while parsing the component config, using default scheduler");
                    new EvenScheduler().schedule(topologies, cluster);
                    return;
                }
                System.out.println("Groups are: " + groups);
                
                // 4. group the executors of the grouped components
                List<List<ExecutorDetails>> groupedExecutors = new ArrayList<List<ExecutorDetails>>();
                for (String groupName : groups.keySet()) {
                    List<String> group = groups.get(groupName);
                    // 4.1 check if the number of executors are the same for all components in the same group
                    // get the num executors of the first component of the group
                    int numExecutors = componentToExecutorsMap.get(group.get(0)).size();
                    // compare the num executors of the other component of the group with
                    for (String component : group) {
                        if (componentToExecutorsMap.get(component).size() != numExecutors) {
                            System.out.println("Component " + component + " does not have the same number of executors as " + group.get(0));
                            System.out.println("Using default scheduler");
                            new EvenScheduler().schedule(topologies, cluster);
                            return;
                        }
                    }
                    // 5. Zip the executors of the group together
                    List<List<ExecutorDetails>> listOfExecutorList = new ArrayList<List<ExecutorDetails>>();
                    for (String componentName : group) {
                        listOfExecutorList.add(componentToExecutorsMap.get(componentName));
                    }
                    groupedExecutors.addAll(Utils.zip(listOfExecutorList));
                }
                System.out.println("Grouped executors " + groupedExecutors);
                
                // 6. Arrange workers accordingly
                List<List<WorkerSlot>> listOfWorkerSlotList = new ArrayList<List<WorkerSlot>>();
                for (String supervisor : supervisorsMap.keySet()) {
                    listOfWorkerSlotList.add(cluster.getAvailableSlots(supervisorsMap.get(supervisor)));
                }
                List<WorkerSlot> workerList = Utils.flattenList(listOfWorkerSlotList);
                
                // 7. get only the enough number of workers
                int numWorkersForTopology = Math.min(workerList.size(), topology.getNumWorkers());
                workerList = workerList.subList(0, numWorkersForTopology);
                
                // 8. partition the executors into the worker bins
                int numGroupsPerWorker = (int) Math.ceil((double) groupedExecutors.size() / workerList.size());
                List<List<List<ExecutorDetails>>> partitionedExecutors = Lists.partition(groupedExecutors, numGroupsPerWorker);
                // 8a. the number of partitions should be <= number of available workers
                if (partitionedExecutors.size() > workerList.size()) {
                    System.out.println("The number of partitions is more than the number of available workers!");
                    System.out.println("Something wrong! ");
                    System.out.println("Using default scheduler");
                    new EvenScheduler().schedule(topologies, cluster);
                    return;
                }
                
                // 9. iterate over the executors and assign it to the workers
                for (int i = 0; i < partitionedExecutors.size(); i++) {
                    cluster.assign(workerList.get(i), topology.getId(), Utils.flattenList(partitionedExecutors.get(i)));
                    System.out.println("Assigned executors:" + partitionedExecutors.get(i) + " to slot: [" + workerList.get(i % numWorkersForTopology).getNodeId() + ", " + workerList.get(i % numWorkersForTopology).getPort() + "]");
                }
            }
        }
        System.out.println("GroupAwareScheduler: done with scheduling");
        System.out.println("========================================================================");
        
        // let system's even scheduler handle the rest scheduling work
        new EvenScheduler().schedule(topologies, cluster);
    }

}