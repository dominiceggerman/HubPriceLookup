# Imports
import pandas as pd
import numpy as np
import psycopg2
import readfile
from datetime import datetime as dt


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
    # Get price data
    prices = accessDB(user, password)
    # Fill NaN and alter dtype
    prices.fillna(0, inplace=True)
    prices["Average Price"] = prices["Average Price"].astype(float)
    
    # Pivot hub
    hub_prices = prices.pivot_table(values="Average Price", index=["Issue Date"], columns=["Price Point Name"])
    # Get day-to-day difference
    hub_prices_diff = hub_prices.diff()
    # Get percentage change day-to-day
    price_percentage = hub_prices_diff / hub_prices

    # Get junk columns to drop
    to_drop = price_percentage.columns[(price_percentage.abs() <= 0.05).iloc[-1]]
    # Drop junk columns
    hub_prices.drop(to_drop, axis=1, inplace=True)
    price_percentage.drop(to_drop, axis=1, inplace=True)

    # Same but with percentages = 1
    to_drop = price_percentage.columns[(price_percentage.abs() == 1.0).iloc[-1]]
    hub_prices.drop(to_drop, axis=1, inplace=True)
    price_percentage.drop(to_drop, axis=1, inplace=True)


    print(hub_prices.tail())
    print(price_percentage.tail())