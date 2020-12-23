# graph-loader

This repository contains a script to demonstrate joining schema.org data from several websites into a single graph that can then be queried.

## Quick Start Tutorial

Follow this instruction to get [Blazegraph](https://blazegraph.com) running inside of a Docker container with biological data from several sources (https://disprot.org, https://mobidb.org, https://www.wikidata.org) loaded from GCS.

1. Make sure Docker is [installed](https://docs.docker.com/engine/install) and [can be run without sudo](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user)

1. Install Python dependencies by running the following command from the root of this repository:
    ```
    pip3 install -r scripts/requirements.txt
    ```
1. Make sure gcloud is [installed](https://cloud.google.com/sdk/docs/install)

1. In order for the Python script to access data in GCS, generate credentials by running the following command:

    ```
    gcloud auth application-default login
    ```

    and following prompted instructions.

1. Initialize new graph by running the following command from the root of this repository:

    ```
    python3 scripts/graph-admin.py initialize_graph --graph blazegraph
    ```

1. Load the data into the graph by running the following command from the root of this repository:

    ```
    python3 scripts/graph-admin.py load_data --graph blazegraph --data_file graph_data.txt
    ```

1. Open Blazegraph web UI by navigating to http://localhost:8885/bigdata/#query

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
## Appendix

### Mounting another web UI

At previous step Blazegraph server and web UI were used. However sometimes another web UI have to be integrated with the same graph data.

Microservice architecture consisting of three containers allows to deploy Blazegraph and generic graph UI ([Yasgui](https://triply.cc/docs/yasgui-api)) on a single machine, locally or in the cloud.

Follow this instruction:

1. Make sure Blazegraph with data was created.

1. Create yasgui microservice by running the following command from the root of this repository:

    ```
    python3 scripts/graph-admin.py run_yasgui --blazegraph_name BLAZEGRAPH_NAME --yasgui_endpoint HOST:PORT
    ```

    External endpoint [HOST:PORT] is an address you will be using to access Yasgui and Blazegraph, it can be e.g. externally accessible address of the virtual machine.

    For example, for blazegraph8885 container (created at previous step) and 127.0.0.1:8888 external endpoint:

    ```
    python3 scripts/graph-admin.py run_yasgui --blazegraph_name blazegraph8885 --yasgui_endpoint 127.0.0.1:8888

    ```

1.  Open external endpoint in browser

### Source data

`graph-admin` can load data both from local files and Google Cloud Storage.

To specify sources create a text file in format specified below and pass the name of this file through the `--data_file` flag.

Each line in the data file represents a single source of data: either a single file or a directory on a local file system or in GCS.

For example to load the data from a single local file add a line with the path to this file:

```
data/disprot.org/graph.nq
```

To load the data from GCS directory add a line with the "gs://" prefix and the full path to this directory (including bucket):
```
gs://wikidata-collab-1-crawler/mobidb.org/single_file
```

You can optionally add a link to the license associated with the data separated by tab symbol.
Then `graph-admin` will print information about the license when loading the data.
```
data/disprot.org/graph.nq	https://creativecommons.org/licenses/by/4.0/
```

### Exporting result data

The result of loading the data is a running container with the graph. You can then export this container to a Docker image which can then be uploaded to a registry and easily distributed from there.
