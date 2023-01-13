# Import Functions
from datetime import datetime, date, timedelta
from utilities.database.db_utils import sql_query

class manage_line():
    def __init__(self):
        pass

    def get_in_line(self, id_analista):
        tbl_hist = sql_query(consult = True, SQL = f"""SELECT COUNT(id_analista) FROM mng_line2 WHERE id_analista = {id_analista}""")
        if tbl_hist[0][0] == 0:
            sql_query(consult = False, SQL = f"""INSERT INTO mng_line2 (id_analista, entry_time) VALUES ({id_analista}, TIMESTAMP '{datetime.now()}')""")
        else:
            sql_query(consult = False, SQL = f"""DELETE FROM mng_line2 WHERE id_analista = {id_analista}""")
            sql_query(consult = False, SQL = f"""INSERT INTO mng_line2 (id_analista, entry_time) VALUES ({id_analista}, TIMESTAMP '{datetime.now()}')""")
    
    def get_out_of_line(self, id_analista):
        sql_query(consult = False, SQL = f"""DELETE FROM mng_line2 WHERE id_analista = {id_analista}""")

    def check_position(self, id_analista):
        tbl_hist = sql_query(consult = True, SQL = f"""
                                                    SELECT MIN(index), entry_time, id_analista,
                                                    (CASE WHEN id_analista = {id_analista} THEN True ELSE False END) retorno
                                                    FROM mng_line2
                                                    GROUP BY index, id_analista, entry_time
                                                    ORDER BY index ASC
                                                    """)

        if not tbl_hist[0][3]:
            elapsed = datetime.combine(date.today(), datetime.now().time()) - datetime.combine(date.today(), tbl_hist[0][1].time())
            if elapsed > timedelta(minutes = 1, seconds=30):
                return self.remove_first(tbl_hist[0][2], id_analista)
        else:
            return True
        
    def remove_first(self, first, id_analista):
        sql_query(consult = False, SQL = f"""DELETE FROM mng_line2 WHERE id_analista = {first}""")
        tbl_hist = sql_query(consult = True, SQL = f"""
                                                SELECT MIN(index), entry_time, id_analista,
                                                (CASE WHEN id_analista = {id_analista} THEN True ELSE False END) retorno
                                                FROM mng_line2
                                                GROUP BY index, id_analista, entry_time
                                                ORDER BY index ASC
                                                """)
        return tbl_hist[0][3]

    def clear_line(self):
        sql_query(consult = False, SQL = f"""DELETE FROM mng_line2""")


