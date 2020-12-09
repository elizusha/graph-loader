# graph-loader

This repository contains a script to demonstrate joining schema.org data from several websites into a single graph that can then be queried.

Follow this instruction to get a graph with biological data from any sources (e.g. https://disprot.org, https://mobidb.org, https://www.wikidata.org) stored in the graph (e.g. [Blazegraph](https://blazegraph.com)) running inside of a Docker container:

1. Make sure Docker is [installed](https://docs.docker.com/engine/install) and [can be run without sudo](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user)
1. Install Python dependencies by running the following command from the root of this repository:
    ```
    pip3 install -r scripts/requirements.txt
    ```
1. Run the graph-admin script from the root of this repository:
    1. with "initialize_graph" command to initialize new graph.
        ```
        python3 scripts/graph-admin.py initialize_graph --graph blazegraph
        ```
        Arguments:

          --graph GRAPH_NAME - choose one of the graphs. "blazegraph" and "agraph" supported

          --port PORT - set graph port

          --remove_previous_graph - remove previous graph instance if exists

    1. with "load_data" command to load new nquads into the graph.
        ```
        python3 scripts/graph-admin.py load_data --graph blazegraph
        ```
        Arguments:

          --graph GRAPH_NAME - choose one of the graphs. "blazegraph" and "agraph" supported

          --data_file FILE_NAME - file with data paths. Default: "graph_data.txt"

          --data_list LIST - data paths

1. Open graph UI
    1. for blazegraph web UI by navigating to http://localhost:8885/bigdata/#query
1. Run queries. For example:
    1. for all types in graph
        ```
        SELECT DISTINCT ?type
        {
            GRAPH ?graph { ?value <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type }
        }
        ```
    1. for all information about a single uniprot protein e.g. protein P49869
        ```
        SELECT DISTINCT ?graph ?property ?value
        {
            GRAPH ?graph { ?protein ?p <https://www.uniprot.org/uniprot/P49869> }
            { ?protein ?property ?value}
        }
        ```
