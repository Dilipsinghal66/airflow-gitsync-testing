from airflow.hooks.mysql_hook import MySqlHook
from contextlib import closing


class CustomMySqlHook(MySqlHook):

    def __init__(self, *args, **kwargs):
        super(CustomMySqlHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def upsert_rows(self, table, rows, target_fields=None, commit_every=1000,
                    unique_fields=[]):
        """

        :param unique_fields: Unique keys(cannot be updated)
        :param table: Table name in the mysql database
        :param rows: List of rows to be upserted
        :param target_fields: Fields to be upserted
        :param commit_every: No. upserts per commit
        :return:
        """
        fields = target_fields

        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = "({})".format(target_fields)
        else:
            target_fields = ''
        i = 0
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            conn.commit()

            with closing(conn.cursor()) as cur:
                for i, row in enumerate(rows, 1):
                    lst = []
                    for cell in row:
                        lst.append(self._serialize_cell(cell, conn))

                    values = tuple(lst)
                    placeholders = ["%s", ] * len(values)

                    sql = "INSERT INTO "
                    sql += "{0} {1} VALUES ({2})".format(
                        table,
                        target_fields,
                        ",".join(placeholders))
                    sql += " ON DUPLICATE KEY UPDATE "

                    update_str = []

                    for ii in range(len(fields)):
                        if fields[ii] not in unique_fields:
                            update_str.append("{0} = '{1}'".format(fields[ii],
                                                                   row[ii]))
                    sql += ", ".join(update_str)
                    self.log.debug(sql)
                    cur.execute(sql, values)
                    if commit_every and i % commit_every == 0:
                        conn.commit()
                        self.log.info(
                            "Loaded %s into %s rows so far", i, table
                        )

            conn.commit()
        self.log.info("Done loading. Loaded a total of %s rows", i)
