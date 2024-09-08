set(sources
		src/DistributedGraph.cpp
)

set(exe_sources
		src/main.cpp
		${sources}
)

set(headers
	include/partitioning_engine/Partitioner.hpp
    include/DistributedGraph.hpp
	include/Graph.hpp
	include/MPI.hpp
	include/Logger.hpp
)	