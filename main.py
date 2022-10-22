import uuid
from datetime import timedelta

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,
                               QueryOptions)

# Update this to your cluster
endpoint = "7vf25rkkxvfttdhz.pvygn93kl9pn6psp.cloud.couchbase.com"
username = "dev"
password = "Eto2022$dev"
bucket_name = "autopyproject"
# User Input ends here.

# Connect options - authentication
auth = PasswordAuthenticator(username, password)

# Connect options - global timeout opts
timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10))

# get a reference to our cluster
cluster = Cluster('couchbases://{}'.format(endpoint),
                  ClusterOptions(auth, timeout_options=timeout_opts))

# Wait until the cluster is ready for use.
cluster.wait_until_ready(timedelta(seconds=5))

# get a reference to our bucket
cb = cluster.bucket(bucket_name)

cb_project = cb.scope("admin").collection("project")


def upsert_document(doc):
    print("\nUpsert CAS: ")
    try:
        # key will equal: "airline_8091"
        key = str(doc["id"])
        result = cb_project.upsert(key, doc)
        print(result.cas)
    except Exception as e:
        print(e)


# get document function


def get_project_by_key(key):
    print("\nGet Result: ")
    try:
        result = cb_project.get(key)
        print(result.content_as[str])
    except Exception as e:
        print(e)


# query for new document by callsign


def lookup_by_callsign(cs):
    print("\nLookup Result: ")
    try:
        sql_query = 'SELECT VALUE name FROM `autopyproject`.admin.project WHERE name = $1'
        row_iter = cluster.query(
            sql_query,
            QueryOptions(positional_parameters=[cs]))
        for row in row_iter:
            print(row)
    except Exception as e:
        print(e)


def test():
    u = str(uuid.uuid4())
    project = {
        "name": "Prj1",
        "id": f"PRJ-{u}",
        "sign": "DEV",
        "description": "project test",
    }

    upsert_document(project)

    get_project_by_key(f"PRJ-{u}")

    lookup_by_callsign("Prj1")


# This is a sample Python script.

# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    test()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
