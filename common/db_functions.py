from airflow.contrib.hooks.mongo_hook import MongoHook


def get_data_from_db(conn_id=None, collection=None, **kwargs):
    coll = MongoHook(
        conn_id=conn_id).get_conn().get_default_database().get_collection(
        collection)
    data = coll.find(kwargs)
    return data
