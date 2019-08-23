from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.hooks.mysql_hook import MySqlHook


def get_data_from_db(db_type="mongo", conn_id=None, collection=None, **kwargs):
    if db_type == "mongo":
        coll = MongoHook(
            conn_id=conn_id).get_conn().get_default_database().get_collection(
            collection)
        data = coll.find(**kwargs)
    if db_type == "mysql":
        #sql = kwargs.get("sql_query")
        data = MySqlHook(mysql_conn_id=conn_id)
        #data = db.get_records(sql=sql)
    return data
