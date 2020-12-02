import argparse
import subprocess
import requests
import logging
import time
from google.cloud.storage import Client
from rdflib import Graph, URIRef, Literal, ConjunctiveGraph
from typing import Iterator, List
from urllib.parse import quote_plus


def download_files(
    client: Client, bucket_name: str, blobs_directory_name: str
) -> Iterator[str]:
    logging.info(f"Using following GCS bucket: '{bucket_name}'")
    logging.info(f"Using following GCS directory: '{blobs_directory_name}'")
    bucket = client.bucket(bucket_name)
    all_blobs = list(client.list_blobs(bucket, prefix=blobs_directory_name))
    for blob in all_blobs:
        logging.info(f"Downloading '{blob.name}'")
        if blob.name[-3:] == ".nq":
            data = blob.download_as_string().decode()
            logging.info(f"Succesfully downloaded: '{blob.name}'")
            yield data
        else:
            logging.warning(
                f"Not downloading '{blob.name}': it is not .nq file")


def parse_graph(file_contents: str) -> ConjunctiveGraph:
    graph = ConjunctiveGraph(store="IOMemory")
    graph.parse(data=file_contents, format="nquads")
    return graph


def build_insert_queries(graph: ConjunctiveGraph) -> List[str]:
    for term in graph.quads():
        graph_name = term[3].identifier
        break
    nt_data = graph.serialize(format="nt").decode()
    logging.info(f"Graph name is {graph_name}")
    nts = nt_data.split("\n")
    queries = []
    MAX_QUERY_LENGTH: int = 200000 - 100
    query_len = 0
    query_data = []
    for i in range(0, len(nts)):
        nq_encoded_len = len(quote_plus(nts[i]+"\n"))
        if query_len + nq_encoded_len > MAX_QUERY_LENGTH:
            query_data_str = "\n".join(query_data)
            queries.append(
                f"INSERT DATA {{ GRAPH <{graph_name}> {{ {query_data_str} }} }}")
            query_len = 0
            query_data = []
        query_len += nq_encoded_len
        query_data.append(nts[i])
    query_data_str: str = '\n'.join(query_data)
    queries.append(
        f"INSERT DATA {{ GRAPH <{graph_name}> {{ {query_data_str} }} }}")
    return queries


def insert_data(blazegraph_url: str, insert_query: str) -> None:
    res = requests.post(blazegraph_url, data={"update": insert_query})
    if not res.ok:
        logging.warning(f"Failed to insert data into graph: {res}")


def load_data_from_cloud(args):
    client = Client.create_anonymous_client()
    blazegraph_url = f"http://localhost:{args.port}/bigdata/namespace/kb/sparql"
    with open("../graphs_data.txt") as data_file:
        for path in data_file.readlines():
            bucket_name, blobs_directory_name = path.strip().split("/", maxsplit=1)
            for file_contents in download_files(
                client, bucket_name, blobs_directory_name
            ):
                graph: ConjunctiveGraph = parse_graph(file_contents)
                insert_queries: List[str] = build_insert_queries(graph)
                for i, query in enumerate(insert_queries):
                    logging.info(
                        f"Running insert query {i+1} / {len(insert_queries)}")
                    insert_data(blazegraph_url, query)


def initialize_blazegraph(args):
    container_name = f"blazegraph{args.port}"
    if args.remove_previous_graph:
        logging.info(
            f"Removing blazegraph container {container_name}")
        remove_graph_command = [
            "docker",
            "rm",
            "-f",
            container_name
        ]
        process = subprocess.run(
            remove_graph_command, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, encoding="utf-8")
        if process.returncode != 0:
            logging.error(
                f"Failed to remove blazegraph container. Docker output:\n{process.stdout}")
            raise Exception("Failed to remove blazegraph container")
        logging.info(f"Container {container_name} removed.")

    logging.info(
        f"Running blazegraph container {container_name} on host port {args.port}")
    run_graph_command = [
        "docker",
        "run",
        "--name",
        container_name,
        "-d",
        "-p",
        f"{args.port}:8080",
        "lyrasis/blazegraph:2.1.5",
    ]
    process = subprocess.run(
        run_graph_command, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, encoding="utf-8")
    if process.returncode != 0:
        logging.error(
            f"Failed to create blazegraph container. Docker output:\n{process.stdout}")
        raise Exception("Failed to create blazegraph container")
    logging.info(
        f"Blazegraph container created. Listening on host port {args.port}")
    # Blazegraph takes some time to start.
    time.sleep(5)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port",
        help="blazegraph server port",
        default="8885",
    )
    parser.add_argument(
        "--remove_previous_graph",
        action="store_true",
        default=False,
        help="remove previous blazegraph if exists",
    )
    return parser.parse_args()


def _configure_logging():
    FORMAT = "%(asctime)-15s %(levelname)s: %(message)s"
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    logging.basicConfig(
        format=FORMAT,
        level=logging.DEBUG,
        handlers=[
            stream_handler,
            logging.FileHandler(f"../loader.log"),
        ],
    )


def main():
    args = parse_args()
    _configure_logging()
    initialize_blazegraph(args)
    load_data_from_cloud(args)


if __name__ == "__main__":
    main()
