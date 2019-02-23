from airflow.contrib.hooks.mongo_hook import MongoHook


DATABASE_TYPES_MAP = {
 "mongo": {"class": MongoHook, "conn_id": "mongo_default"}
}


class BaseDatabaseModel(object):

    def __init__(self, database_type="mongo", connection_id=None):
        """

        :param database_type:
        :param connection_id:
        """
        if not database_type:
            raise ValueError()
        self.db = self.__get_db(database_type=database_type,
                                connection_id=connection_id)

    def __get_db(self, database_type, connection_id):
        """

        :param database_type:
        :param connection_id:
        :return:
        """
        db_config = DATABASE_TYPES_MAP.get(database_type, None)
        if db_config:
            db_class = db_config.get("class", None)
            if not db_class:
                raise NotImplementedError()
            if not connection_id:
                connection_id = db_config.get("conn_id")
        return db_class(conn_id=connection_id)
