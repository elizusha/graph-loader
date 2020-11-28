import argparse
import subprocess
import requests
import logging
import time
from google.cloud.storage import Client
from rdflib import Graph, URIRef, Literal, ConjunctiveGraph
from typing import Iterator, List
from urllib.parse import urlencode


def download_files(
    client: Client, bucket_name: str, blobs_directory_name: str
) -> Iterator[str]:
    logging.info(f"Using following GCS bucket: {bucket_name}")
    logging.info(f"Using following GCS directory: {blobs_directory_name}")
    bucket = client.bucket(bucket_name)
    all_blobs = list(client.list_blobs(bucket, prefix=blobs_directory_name))
    for blob in all_blobs:
        logging.info(f"Downloading {blob.name}")
        if blob.name[-3:] == ".nq":
            data = blob.download_as_string().decode()
            logging.info(f"Succesfully downloaded: {blob.name}")
            yield data
        else:
            logging.warning(f" {blob.name} is not .nq file")


def parse_graph(file_contents: str) -> ConjunctiveGraph:
    graph = ConjunctiveGraph(store="IOMemory")
    graph.parse(data=file_contents, format="nquads")
    return graph


def build_insert_queries(graph: ConjunctiveGraph) -> List[str]:
    for term in graph.quads():
        graph_name = term[3].identifier
        break
    nt_data = graph.serialize(format="nt").decode()
    logging.info(f"GRAPH: {graph_name}")
    nts = nt_data.split("\n")
    queries = []
    step = 1000
    for i in range(0, len(nts), step):
        nt_part = "\n".join(nts[i:min(i+step, len(nts))])
        queries.append(f"INSERT DATA {{ GRAPH <{graph_name}> {{ {nt_part} }} }}")
    return queries


def insert_data(blazegraph_url: str, insert_query: str) -> None:
    res = requests.post(blazegraph_url, data={"update": insert_query})
    logging.info(f"GRAPH: Insert data. Response: {res}")


def load_data_from_cloud(args):
    client = Client()
    if args.remove_previous_graph:
        run_graph_command = [
            "docker",
            "rm",
            "-f",
            f"blazegraph{args.port}"
        ]
        run_graph_command_output = subprocess.run(run_graph_command, capture_output=True)
        logging.info(f"GRAPH: Blazegraph{args.port} removed.")
    logging.info(f"GRAPH: Run blazegraph.")
    run_graph_command = [
        "docker",
        "run",
        "--name",
        f"blazegraph{args.port}",
        "-d",
        "-v",
        "/home/elizusha/graph-loader/data:/stuff",
        "-p",
        f"{args.port}:8080",
        "lyrasis/blazegraph:2.1.5",
    ]
    run_graph_command_output = subprocess.run(run_graph_command, capture_output=True)
    logging.info(f"GRAPH: Blazegraph created. Port: {args.port}")
    time.sleep(5)
    blazegraph_url = f"http://localhost:{args.port}/bigdata/namespace/kb/sparql"
    with open("../graphs_data.txt") as data_file:
        for path in data_file.readlines():
            bucket_name, blobs_directory_name = path.strip().split("/", maxsplit=1)
            for file_contents in download_files(
                client, bucket_name, blobs_directory_name
            ):
                graph: ConjunctiveGraph = parse_graph(file_contents)
                insert_queries: str = build_insert_queries(graph)
                for query in insert_queries:
                    insert_data(blazegraph_url, query)


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
    load_data_from_cloud(args)


if __name__ == "__main__":
    main()
