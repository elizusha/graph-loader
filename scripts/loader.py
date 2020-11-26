import argparse
import subprocess
import requests
from google.cloud import storage
from rdflib import Graph, URIRef, Literal, ConjunctiveGraph
# logs

def download_blobs(storage_client, bucket_name, blobs_directory_name, graph):
    print("BUCKET:", bucket_name)
    print("DIRECTORY:", blobs_directory_name)
    bucket = storage_client.bucket(bucket_name)
    all_blobs = list(storage_client.list_blobs(bucket, prefix=blobs_directory_name))
    for blob in all_blobs:
        print(f"BLOB: {blob.name}")
        nquads = blob.download_as_string().decode()
        graph.parse(
            data=nquads,
            format="nquads"
        )
        for term in graph:
            graph_name = term[0]
            break
        nt_data = graph.serialize(format="nt").decode()
        query = f"INSERT DATA {{ GRAPH <{graph_name}> {{ {nt_data} }} }}"
        requests.post("http://localhost:8888/bigdata/namespace/kb/sparql", data={ "update" : query })


def load_data_from_cloud(args):
    storage_client = storage.Client()
    # run_graph_command = ["docker", "run", "--name", "blazegraph8888", "-d", "-v", "/home/elizusha/graph-loader/data:/stuff", "-p", "8888:8080", "lyrasis/blazegraph:2.1.5"]
    # run_graph_command_output = subprocess.run(run_graph_command, capture_output=True)
    # print("***Graph created***")

    graph = ConjunctiveGraph(store="IOMemory")

    with open("../graphs_data.txt") as data_file:
        for path in data_file.readlines():
            bucket_name, blobs_directory_name = path.split('/', maxsplit=1)
            download_blobs(storage_client, bucket_name, blobs_directory_name, graph)


def parse_args():
    parser = argparse.ArgumentParser() # port
    return parser.parse_args()


def main():
    args = parse_args()
    load_data_from_cloud(args)

if __name__ == "__main__":
    main()
