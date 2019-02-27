import pyodbc
connstr = 'Driver={SQL Server Native Client 11.0};Server=localhost;Database=ggbet;uid=localdev;pwd=asdf1234'
connstr = 'Driver={SQL Server Native Client 11.0};Server=esports-st.database.windows.net;Database=mainDb;uid=bodboy52;pwd=Chelsea12'
connstr = 'Driver={SQL Server Native Client 11.0};Server=localhost\SQLEXPRESS;Database=ggbet;uid=localdev;pwd=asdf1234'

def select(query):
    try:
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor().execute("SET NOCOUNT ON;"+query)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        conn.commit()
        return results
    except Exception as ex:
        print(str(ex))
        raise 
    finally:
        cursor.close()
        conn.close()

def execute(query):
    try:
        conn = pyodbc.connect(connstr)
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.commit()
    except Exception as ex:
        print(str(ex))
        raise 
    finally:
        cursor.close()
        conn.close()
