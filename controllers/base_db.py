from models.base_db import BaseDatabaseModel


class DatabaseController(BaseDatabaseModel):

    def __init__(self, database_type="mongo", connection_id="mongo_default"):
        """

        :param database_type:
        :param connection_id:
        """
        super(DatabaseController, self).__init__(database_type=database_type,
                                                 connection_id=connection_id)
