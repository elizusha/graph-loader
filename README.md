# graph-loader

This repository contains a script to demonstrate joining schema.org data from several websites into a single graph that can then be queried.

Follow this instruction to get a graph with biological data from three sources (https://disprot.org, https://mobidb.org, https://www.wikidata.org) stored in [Blazegraph](https://blazegraph.com) running inside of a Docker container:

1. Make sure Docker is [installed](https://docs.docker.com/engine/install) and [can be run without sudo](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user)
1. Install Python dependencies by running the following command from the root of this repository: 
    ```
    pip3 install -r scripts/requirements.txt
    ```
1. Run the loader script from the root of this repository:
    ```
    python3 scripts/loader.py
    ```
1. Wait for the loader script to finish.
1. Open blazegraph web UI by navigating to http://localhost:8885/bigdata/#query
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
