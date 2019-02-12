# Imports
import pandas as pd
import numpy as np
import psycopg2
import readfile


# Query Zones
def accessDB(user, password, zone1, zone2):
    # Connect
    conn = psycopg2.connect(dbname="gasaprod", user=user, password=password, host="gasaproddb02.genscape.com", port="5432")
    cursor = conn.cursor()

    # SQL
    sql = """SELECT nd.issue_date, npp.price_point_name, npp.ngi_region_name, nd.average 
            FROM ts1.ngi_daily AS nd
            INNER JOIN ts1.ngi_price_point AS npp ON npp.pointcode = nd.pointcode
            WHERE nd.trade_date BETWEEN current_date - INTERVAL '120 days' AND current_date
            AND npp.price_point_name IN ('[zone1]', '[zone2]')
        """.replace("[zone1]", zone1).replace("[zone2]", zone2)

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

    # HH vs HSC
    prices = accessDB(user, password, "Henry Hub", "Houston Ship Channel")

    # Fill NaN and alter dtype
    prices.fillna(0, inplace=True)
    prices["Average Price"] = prices["Average Price"].astype(float)

    zone_prices = prices.pivot_table(values="Average Price", index=["Issue Date"], columns=["Price Point Name"])

    print(zone_prices.tail())