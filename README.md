### Group Aware Scheding of Storm Topology ###

The storm scheduler is responsible for assignment of of the components (bolts and spouts)
to the processes and the machines they run on. A scheduler works in two phases
  i) it first assigns the components (each tied to an executor) to workers (JVM processes)
  ii) and then assigns these workers to the supervisors (nodes)
The default storm scheduler follows round-robin strategy during each of these assignments
This, typically, helps balance out the resource usage; but not always.
When the tuples emitted by either a spout or a bolt is too big, and the participating
components are on different physical machines, network latency increases thus decreasing
throughput.

This scheduler assigns the components of the same group to the same processes. They are
only  grouped logically and not tied to a particular machine. The default scheduler takes
up the job of assigning processes to machines.
The components can be added to the same group by specifying the same group ID via the
addConfiguration() while defining the topology. (See example topology)

# TODO: topology name confiuration instruction
# TODO: add jar export instruction installation instruction
# TODO: add storm config instruction
