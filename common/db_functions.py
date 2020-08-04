from airflow.contrib.hooks.mongo_hook import MongoHook
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from common.custom_hooks.custom_sql_hook import CustomMySqlHook


def get_data_from_db(db_type="mongo", conn_id=None, collection=None,
                     query_type="select", **kwargs):
    if db_type == "mongo":
        coll: Collection = MongoHook(
            conn_id=conn_id).get_conn().get_default_database().get_collection(
            collection)
        data: Cursor = coll.find(**kwargs)
        if query_type == "distinct":
            distinct_key = kwargs.get("query_filter", None)
            if not distinct_key:
                raise ValueError("query_filter is required when query_type "
                                 "is distinct")
            data = data.distinct(key=distinct_key)
    if db_type == "mysql":
        data: CustomMySqlHook = CustomMySqlHook(mysql_conn_id=conn_id)
        execute_query = kwargs.get("execute_query", False)
        if execute_query:
            sql = kwargs.get("sql_query")
            data = data.get_records(sql=sql)
    return data
