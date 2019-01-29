# Imports
import pandas as pd
import psycopg2
import readfile


# Access DB
def accessDB(user, password):
    # Connect
    conn = psycopg2.connect(dbname="gasaprod", user=user, password=password, host="gasaproddb02.genscape.com", port="5432")
    cursor = conn.cursor()

    # SQL
    sql = """SELECT nd.issue_date, npp.price_point_name, npp.ngi_region_name, nd.average 
            FROM ts1.ngi_daily AS nd
            INNER JOIN ts1.ngi_price_point AS npp ON npp.pointcode = nd.pointcode
            WHERE nd.trade_date BETWEEN current_date - INTERVAL '120 days' AND current_date
        """

    # Execute SQL
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        print("Error while executing SQL...")
        return None

    # Close and return
    cursor.close()
    return pd.DataFrame(results, columns=["Issue Date", "Price Point Name", "Region Name", "Average Price"])


# Run
if __name__ == "__main__":
    # Get creds
    user, password = readfile.readFile("creds.txt")
    # Get data
    prices = accessDB(user, password)
    
    # Get average price over period
    hub_prices = prices.fillna(0).pivot_table(values="Average Price", index=["Issue Date"], columns=["Price Point Name"])
    print(hub_prices.head())