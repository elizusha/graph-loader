import argparse
import subprocess
import requests
import logging
import time
import os
import os.path
import tempfile
import re
from glob import glob
from google.cloud.storage import Client, Bucket, Blob
from rdflib import Graph, URIRef, Literal, ConjunctiveGraph
from typing import Iterator, List, NamedTuple, Optional, Set
from urllib.parse import quote_plus


gcs_client: Optional[Client] = None


class FileContent(NamedTuple):
    file_name: str
    data: str


def create_gcs_client() -> Client:
    global gcs_client
    if gcs_client is None:
        try:
            gcs_client = Client()
        except Exception as e:
            logging.info(
                f"Failed to create authenticated gcs client, defaulting to anonymous. Error:\n{e}"
            )
            gcs_client = Client.create_anonymous_client()
    return gcs_client


def download_files(path: str) -> Iterator[FileContent]:
    GCS_PATH_PREFIX: str = "gs://"

    if path.startswith(GCS_PATH_PREFIX):
        return download_gcs_files(path[len(GCS_PATH_PREFIX) :])
    else:
        return download_local_files(path)


def download_local_files(path: str) -> Iterator[FileContent]:
    logging.info(f"Using following local path: '{path}'")
    file_paths: List[str]
    if os.path.isfile(path):
        file_paths = [path]
    elif os.path.isdir(path):
        file_paths = glob(os.path.join(path, "*.nq"))
    else:
        logging.warning(f"Not downloading '{path}': wrong format")
    for file_path in file_paths:
        if file_path.endswith(".nq"):
            logging.info(f"Loading '{file_path}'")
            with open(file_path) as file:
                data: str = file.read()
                yield FileContent(file_path, data)
        else:
            logging.warning(f"Not downloading '{file_path}': it is not .nq file")


def download_gcs_files(path: str) -> Iterator[FileContent]:
    client: Client = create_gcs_client()
    bucket_name, blobs_path = path.split("/", maxsplit=1)
    logging.info(f"Using following GCS bucket: '{bucket_name}'")
    logging.info(f"Using following GCS path: '{blobs_path}'")
    try:
        bucket: Bucket = client.bucket(bucket_name)
        all_blobs: List[Blob] = list(client.list_blobs(bucket, prefix=blobs_path))
    except Exception as e:
        logging.error(f"Failed to list files in bucket, skipping path. Error:\n{e}")
        return
    if not all_blobs:
        logging.warning(
            f"GCS path error: '{blobs_path}' not found or doesn't contain nq files"
        )
    for blob in all_blobs:
        logging.info(f"Downloading '{blob.name}'")
        if blob.name[-3:] == ".nq":
            data: str = blob.download_as_string().decode()
            logging.info(f"Succesfully downloaded: '{blob.name}'")
            yield FileContent(os.path.join(bucket_name, blob.name), data)
        else:
            logging.warning(f"Not downloading '{blob.name}': it is not .nq file")


def parse_graph(file_content: str) -> ConjunctiveGraph:
    graph = ConjunctiveGraph(store="IOMemory")
    graph.parse(data=file_content, format="nquads")
    return graph


def build_blazegraph_insert_queries(graph: ConjunctiveGraph) -> List[str]:
    MAX_QUERY_LENGTH: int = 200000 - 100

    for term in graph.quads():
        graph_name: str = term[3].identifier
        break
    nt_data: str = graph.serialize(format="nt").decode()
    logging.info(f"Graph name is {graph_name}")
    nts: List[str] = nt_data.split("\n")
    queries: List[str] = []
    query_len: int = 0
    query_data: List[str] = []
    query_lenth_limit: int = MAX_QUERY_LENGTH - len(graph_name)
    for i in range(0, len(nts)):
        nq_encoded_len: int = len(quote_plus(nts[i] + "\n"))
        if query_len + nq_encoded_len > query_lenth_limit:
            query_data_str: str = "\n".join(query_data)
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


class DataInfo(NamedTuple):
    path: str
    license_url: str

    @classmethod
    def parse(cls, data_info_str: str) -> "DataInfo":
        chunks: List[str] = data_info_str.split("\t")
        path: str = chunks[0].strip()
        license: str = ""
        if len(chunks) > 1:
            license = chunks[1].strip()
        return cls(path, license)


def get_data_directories(args) -> List[DataInfo]:
    directories: List[str] = []
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
    return [DataInfo.parse(data_info_str) for data_info_str in directories]


def print_license(license: str) -> None:
    if license:
        logging.info(f"License: {license}")


def load_data(args, data_directories: List[DataInfo]) -> None:
    for data_info in data_directories:
        print_license(data_info.license_url)
        for file_content in download_files(data_info.path):
            graph: ConjunctiveGraph = parse_graph(file_content.data)
            if args.graph == "blazegraph":
                blazegraph_url: str = (
                    f"http://localhost:{args.port}/bigdata/namespace/kb/sparql"
                )
                insert_queries: List[str] = build_blazegraph_insert_queries(graph)
                for i, query in enumerate(insert_queries):
                    logging.info(f"Running insert query {i+1} / {len(insert_queries)}")
                    insert_data(blazegraph_url, query)
            elif args.graph == "agraph":
                raise Exception("Not implemented")


def run_docker_command(command: List[str]) -> str:
    process = subprocess.run(
        command,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        encoding="utf-8",
    )
    if process.returncode != 0:
        logging.error(
            f"Failed to run Docker container command. Docker output:\n{process.stdout}"
        )
        raise Exception("Failed to run Docker container command.")
    return process.stdout.strip()


def remove_previous_graph(args) -> None:
    container_name: str = f"blazegraph{args.port}"
    logging.info(f"Removing blazegraph container {container_name}")
    graph_command: List[str] = ["docker", "rm", "-f", container_name]
    run_docker_command(graph_command)
    logging.info(f"Container {container_name} removed.")


def initialize_blazegraph(args) -> None:
    if args.remove_previous_graph:
        remove_previous_graph(args)
    container_name: str = f"blazegraph{args.port}"
    logging.info(
        f"Running blazegraph container {container_name} on host port {args.port}"
    )
    graph_command: List[str] = [
        "docker",
        "run",
        "--name",
        container_name,
        "-d",
        "-p",
        f"{args.port}:8080",
        "lyrasis/blazegraph:2.1.5",
    ]
    run_docker_command(graph_command)
    logging.info(f"Blazegraph container created. Listening on host port {args.port}")
    # Blazegraph takes some time to start.
    time.sleep(5)


def initialize_agraph(args) -> None:
    raise Exception("Not implemented")


def run_yasgui(args) -> None:
    # ---Blazegraph IP---
    blazegraph_ip_command: List[str] = [
        "docker",
        "inspect",
        "--format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'",
        args.blazegraph_name,
    ]
    blazegraph_ip = run_docker_command(blazegraph_ip_command).replace("'", "")
    logging.info(f"Blazegraph IP: {blazegraph_ip}")

    # ---run yasgui---
    yasgui_command: List[str] = [
        "docker",
        "run",
        "-d",
        "--env",
        f"DEFAULT_SPARQL_ENDPOINT=http://{args.yasgui_endpoint}/blazegraph/bigdata/sparql",
        "erikap/yasgui",
    ]
    yasgui_container_name = run_docker_command(yasgui_command).split("\n")[-1]
    logging.info(f"Yasgui container created. Container name: {yasgui_container_name}")

    # ---Yasgui IP---
    yasgui_ip_command: List[str] = [
        "docker",
        "inspect",
        "--format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'",
        yasgui_container_name,
    ]

    yasgui_ip = run_docker_command(yasgui_ip_command).replace("'", "")

    logging.info(f"Yasgui IP: {yasgui_ip}")

    # ---start nginx---
    nginx_port = args.yasgui_endpoint.split(":")[1]
    nginx_command: List[str] = [
        "docker",
        "run",
        "-p",
        f"{nginx_port}:80",
        "-d",
        "nginx",
    ]
    nginx_container_name = run_docker_command(nginx_command).split("\n")[-1]
    logging.info(f"nginx container created. Container name: {nginx_container_name}")

    # ---default.conf---
    conf_content = f"""server {{
    listen       80;
    listen  [::]:80;
    server_name  localhost;

    location /blazegraph/ {{
        proxy_pass http://{blazegraph_ip}:8080/;
    }}

    location / {{
        proxy_pass http://{yasgui_ip}/;
    }}

    error_page   500 502 503 504  /50x.html;
    location = /50x.html {{
        root   /usr/share/nginx/html;
    }}
}}
    """
    conf_file_path = os.path.join(tempfile.gettempdir(), "default.conf")
    with open(conf_file_path, "w") as file:
        file.write(conf_content)

    # ---mount conf---
    cp_conf_command: List[str] = [
        "docker",
        "cp",
        conf_file_path,
        f"{nginx_container_name}:/etc/nginx/conf.d/default.conf",
    ]
    run_docker_command(cp_conf_command)
    logging.info(f"default.conf file added.")

    # ---reload nginx---
    nginx_reloading_command: List[str] = [
        "docker",
        "exec",
        "-ti",
        nginx_container_name,
        "nginx",
        "-s",
        "reload",
    ]
    run_docker_command(nginx_reloading_command)
    logging.info(f"nginx reloaded.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        help="Graph-admin command. initialize_graph, remove_previous_graph and load_data commands supported",
    )
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
        default="blazegraph",
        help="Graph to load data. Only blazegraph is currently supported",
    )
    parser.add_argument(
        "--yasgui_endpoint",
        default="127.0.0.1:8888",
        help='Yasgui external endpoint. Format: "HOST:PORT". Only for run_yasgui command.',
    )
    parser.add_argument(
        "--blazegraph_name",
        default="blazegraph8885",
        help="Graph name for yasgui queries. Only for run_yasgui command.",
    )
    return parser.parse_args()


def configure_logging() -> None:
    FORMAT: str = "%(asctime)-15s %(levelname)s: %(message)s"
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
    configure_logging()
    if args.command == "initialize_graph":
        if args.graph == "blazegraph":
            initialize_blazegraph(args)
        elif args.graph == "agraph":
            initialize_agraph(args)
    elif args.command == "load_data":
        data_directories = get_data_directories(args)
        load_data(args, data_directories)
    elif args.command == "remove_previous_graph":
        if args.graph == "blazegraph":
            remove_previous_graph(args)
    elif args.command == "run_yasgui":
        run_yasgui(args)
    elif args.command is None:
        logging.error(f"Admin command not found.")
        raise Exception("Admin command not found.")
    else:
        logging.error(f"Unknown command: {args.graph}")
        raise Exception("Unknown command.")


if __name__ == "__main__":
    main()
