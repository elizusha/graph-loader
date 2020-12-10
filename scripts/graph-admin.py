import argparse
import subprocess
import requests
import logging
import time
from google.cloud.storage import Client
from rdflib import Graph, URIRef, Literal, ConjunctiveGraph
from typing import Iterator, List, NamedTuple
from urllib.parse import quote_plus
import os.path
import tempfile

def create_gcs_client():
    try:
        return Client()
    except Exception as e:
        logging.info(
            f"Failed to create authenticated gcs client, defaulting to anonymous. Error:\n{e}"
        )
        return Client.create_anonymous_client()


def download_files(path: str) -> Iterator[str]:
    client = create_gcs_client()
    bucket_name, blobs_directory_name = path.split("/", maxsplit=1)
    logging.info(f"Using following GCS bucket: '{bucket_name}'")
    logging.info(f"Using following GCS directory: '{blobs_directory_name}'")
    bucket = client.bucket(bucket_name)
    all_blobs = list(client.list_blobs(bucket, prefix=blobs_directory_name))
    if not all_blobs:
        logging.warning(
            f"GCS path error: '{blobs_directory_name}' not found or doesn't contain nq files"
        )
    for blob in all_blobs:
        logging.info(f"Downloading '{blob.name}'")
        if blob.name[-3:] == ".nq":
            data = blob.download_as_string().decode()
            logging.info(f"Succesfully downloaded: '{blob.name}'")
            yield data
        else:
            logging.warning(f"Not downloading '{blob.name}': it is not .nq file")


def parse_graph(file_contents: str) -> ConjunctiveGraph:
    graph = ConjunctiveGraph(store="IOMemory")
    graph.parse(data=file_contents, format="nquads")
    return graph


def build_blazegraph_insert_queries(graph: ConjunctiveGraph) -> List[str]:
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
        nq_encoded_len = len(quote_plus(nts[i] + "\n"))
        if query_len + nq_encoded_len > MAX_QUERY_LENGTH:
            query_data_str = "\n".join(query_data)
            queries.append(
                f"INSERT DATA {{ GRAPH <{graph_name}> {{ {query_data_str} }} }}"
            )
            query_len = 0
            query_data = []
        query_len += nq_encoded_len
        query_data.append(nts[i])
    query_data_str: str = "\n".join(query_data)
    queries.append(f"INSERT DATA {{ GRAPH <{graph_name}> {{ {query_data_str} }} }}")
    return queries


def insert_data(blazegraph_url: str, insert_query: str) -> None:
    res = requests.post(blazegraph_url, data={"update": insert_query})
    if not res.ok:
        logging.warning(f"Failed to insert data into graph: {res}")


class DataDirectory(NamedTuple):
    path: str
    license_url: str

    @classmethod
    def parse(cls, data_directory_str: str) -> "DataDirectory":
        chunks = data_directory_str.split("\t")
        path = chunks[0].strip()
        license = ""
        if len(chunks) > 1:
            license = chunks[1].strip()
        return cls(path, license)


def get_data_directories(args) -> List[DataDirectory]:
    directories = []
    if args.data_list:
        directories = [dir.strip() for dir in args.data_list.split(",")]
    elif args.data_file:
        try:
            with open(args.data_file) as data_file:
                directories = [dir.strip() for dir in data_file.readlines()]
        except Exception as e:
            logging.error(f"Failed to read paths from data file. Output:\n{e}")
            raise Exception("Failed to read paths from data file.")
    else:
        logging.error(f"One of --data_list or --data_file required.")
        raise Exception("One of --data_list or --data_file required.")
    return [
        DataDirectory.parse(data_directory_str) for data_directory_str in directories
    ]


def print_license(license):
    if license:
        logging.info(f"License: {license}")


def load_data_from_cloud(args, data_directories):
    for data_directory in data_directories:
        print_license(data_directory.license_url)
        for file_contents in download_files(data_directory.path):
            graph: ConjunctiveGraph = parse_graph(file_contents)
            if args.graph == "blazegraph":
                blazegraph_url = f"http://localhost:{args.port}/bigdata/namespace/kb/sparql"
                insert_queries: List[str] = build_blazegraph_insert_queries(graph)
                for i, query in enumerate(insert_queries):
                    logging.info(f"Running insert query {i+1} / {len(insert_queries)}")
                    insert_data(blazegraph_url, query)
            elif args.graph == "agraph":
                raise Exception("Not implemented")


def remove_previous_graph(args):
    container_name = f"blazegraph{args.port}"
    logging.info(f"Removing blazegraph container {container_name}")
    remove_graph_command = ["docker", "rm", "-f", container_name]
    process = subprocess.run(
        remove_graph_command,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        encoding="utf-8",
    )
    if process.returncode != 0:
        logging.error(
            f"Failed to remove blazegraph container. Docker output:\n{process.stdout}"
        )
        raise Exception("Failed to remove blazegraph container")
    logging.info(f"Container {container_name} removed.")


def initialize_blazegraph(args):
    if args.remove_previous_graph:
        remove_previous_graph(args)
    container_name = f"blazegraph{args.port}"
    logging.info(
        f"Running blazegraph container {container_name} on host port {args.port}"
    )
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
        run_graph_command,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        encoding="utf-8",
    )
    if process.returncode != 0:
        logging.error(
            f"Failed to create blazegraph container. Docker output:\n{process.stdout}"
        )
        raise Exception("Failed to create blazegraph container")
    logging.info(f"Blazegraph container created. Listening on host port {args.port}")
    # Blazegraph takes some time to start.
    time.sleep(5)


def initialize_agraph(args):
    raise Exception("Not implemented")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', help='Graph-admin command. initialize_graph, remove_previous_graph and load_data commands supported')
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
    parser.add_argument(
        "--data_file",
        help="file with nq directories in gcs",
    )
    parser.add_argument(
        "--data_list",
        help="nq directories in gcs",
    )
    parser.add_argument(
        "--graph",
        help="Graph to load data. blazegraph and araph types supported",
    )
    return parser.parse_args()


def _configure_logging():
    FORMAT = "%(asctime)-15s %(levelname)s: %(message)s"
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    logfile = os.path.join(tempfile.gettempdir(), "graph-admin.log")
    logging.basicConfig(
        format=FORMAT,
        level=logging.DEBUG,
        handlers=[
            stream_handler,
            logging.FileHandler(logfile),
        ],
    )
    logging.info(f"Writing logs to {logfile}")


def main():
    args = parse_args()
    _configure_logging()
    if args.command == "initialize_graph":
        if args.graph == "blazegraph":
            initialize_blazegraph(args)
        elif args.graph == "agraph":
            initialize_agraph(args)
        elif not args.graph:
            logging.warning(f"No graph selected. Default graph used: Blazegraph")
            initialize_blazegraph(args)
            args.graph = "blazegraph"  # TODO
        else:
            logging.error(f"Unknown graph type {args.graph}")
            raise Exception("Unknown graph type.")
    elif args.command == "load_data":
        data_directories = get_data_directories(args)
        load_data_from_cloud(args, data_directories)
    elif args.command == "remove_previous_graph":
        remove_previous_graph(args)
    elif args.command is None:
        logging.error(f"Admin command not found.")
        raise Exception("Admin command not found.")
    else:
        logging.error(f"Unknown command: {args.graph}")
        raise Exception("Unknown command.")


if __name__ == "__main__":
    main()
