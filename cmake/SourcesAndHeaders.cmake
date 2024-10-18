set(sources
		src/DistributedGraph.cpp
		src/Workers.cpp
		src/GraphAlgorithm.cpp
		src/graph_algorithms/pr.cpp
		src/graph_algorithms/sssp.cpp
		src/graph_algorithms/cc.cpp

)

set(exe_sources
		src/main.cpp
		${sources}
)

set(headers
	include/partitioning_engine/Partitioner.hpp
	include/offload_engine/NDPEngine.hpp
	include/offload_engine/INCEngine.hpp
    include/DistributedGraph.hpp
	include/Graph.hpp
	include/MPI.hpp
	include/Logger.hpp
	include/Workers.hpp
	include/GraphAlgorithm.hpp
	include/graph_algorithms/pr.hpp
	include/graph_algorithms/sssp.hpp
	include/graph_algorithms/cc.hpp
)	