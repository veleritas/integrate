import re
import os
import tarfile
import shutil
import subprocess
import concurrent.futures
import collections
import textwrap

import urllib.request
import py2neo

from time import sleep

import hetio.readwrite
import hetio.neo4j

import hetio.stats


def replace_text(path, find, repl):
    """
    Read a text file, replace the text specified by find with repl,
    and overwrite the file with the modified version.
    """
    with open(path) as read_file:
        text = read_file.read()
    pattern = re.escape(find)
    text = re.sub(pattern, repl, text)
    with open(path, 'wt') as write_file:
        write_file.write(text)


def create_instance(version, db_id, port, overwrite=False):
    """Create neo4j instance"""

    # port is for bolt
    # HTTP port is bolt_port + 1

    # Download neo4j
    filename = '{}-unix.tar.gz'.format(version)
    path = os.path.join('neo4j', filename)
    if not os.path.exists(path):
        print("Downloading neo4j {}".format(version))
        url = 'http://neo4j.com/artifact.php?name={}'.format(filename)
        urllib.request.urlretrieve(url, path)

    # Extract to file
    tar_file = tarfile.open(path, 'r:gz')
    print("Unpacking neo4j")
    tar_file.extractall('neo4j')
    directory = os.path.join('neo4j', '{}_{}'.format(version, db_id))
    if os.path.isdir(directory) and overwrite:
        shutil.rmtree(directory)
    os.rename(os.path.join('neo4j', version), directory)


    # Modify neo4j-server.properties
    path = os.path.join(directory, 'conf', 'neo4j.conf')

    replace_text(path,
        "#dbms.security.auth_enabled=false",
        "dbms.security.auth_enabled=false"
    )

    replace_text(path,
        "#dbms.connector.bolt.listen_address=:7687",
        "dbms.connector.bolt.listen_address=:{}".format(port)
    )

    replace_text(path,
        "#dbms.connector.http.listen_address=:7474",
        "dbms.connector.http.listen_address=:{}".format(port+1)
    )

    replace_text(path,
        "dbms.connector.https.enabled=true",
        "dbms.connector.https.enabled=false"
    )

    return directory

def hetnet_to_neo4j(path, neo4j_dir, port, database_path='data/graph.db'):
    """
    Read a hetnet from file and import it into a new neo4j instance.
    """
    print("\n")
    neo4j_bin = os.path.join(neo4j_dir, 'bin', 'neo4j')
    subprocess.run([neo4j_bin, 'start'])

    sleep(20)

    error = None
    try:
        print("Starting neo4j import")
        graph = hetio.readwrite.read_graph(path)
        uri = py2neo.Graph(
            host="localhost", http_port = port + 1,
            bolt_port = port, bolt = True
        )

        hetio.neo4j.export_neo4j(graph, uri, 1000, 250, show_progress=True)
    except Exception as e:
        error = e
        print(neo4j_dir, e)
    finally:
        subprocess.run([neo4j_bin, 'stop'])

    if error is not None:
        database_dir = os.path.join(neo4j_dir, database_path)
        remove_logs(database_dir)

def remove_logs(database_dir):
    """Should only run when server is shutdown."""
    filenames = os.listdir(database_dir)
    removed = list()
    for filename in filenames:
        if (filename.startswith('neostore.transaction.db') or
            filename.startswith('messages.log')):
            path = os.path.join(database_dir, filename)
            os.remove(path)
            removed.append(filename)

    return removed


def main():
    # read cross validation index
    with open("../crossval_idx.txt", "r") as fin:
        crossval_idx = int(fin.read().strip())

    print("Cross validation index is {}".format(crossval_idx))

    # Options
    neo4j_version = 'neo4j-community-3.1.1'
    db_name = 'rephetio-v2.0'

    port_0 = 7500 + 100*crossval_idx

    # Identify permuted network files
    permuted_filenames = sorted(x for x in os.listdir('data/permuted') if 'hetnet_perm' in x)
    print("Permuted filenames:", permuted_filenames)


    # Initiate Pool
    pool = concurrent.futures.ProcessPoolExecutor(max_workers = 2)
    port_to_future = collections.OrderedDict()


    # Export unpermuted network to neo4j
    neo4j_dir = create_instance(neo4j_version, db_name, port_0, overwrite=True)
    future = pool.submit(hetnet_to_neo4j, path='data/hetnet.json.bz2', neo4j_dir=neo4j_dir, port=port_0)
    port_to_future[port_0] = future


    # Export permuted network to neo4j
    for i, filename in enumerate(permuted_filenames):
        i += 1
        port = port_0 + 10*i
        db_id = '{}_perm-{}'.format(db_name, i)
        neo4j_dir = create_instance(neo4j_version, db_id, port, overwrite=True)
        path = os.path.join('data', 'permuted', filename)
        future = pool.submit(hetnet_to_neo4j, path=path, neo4j_dir=neo4j_dir, port = port)
        port_to_future[port] = future


    # Shutdown pool
    pool.shutdown()
    print('Complete')


    # Print Exceptions
    for port, future in port_to_future.items():
        exception = future.exception()
        if exception is None:
            continue

        print('\nERROR: Exception importing on port {}:'.format(port))
        print(exception)

if __name__ == "__main__":
    main()
