# Imports
import pandas as pd
import numpy as np
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
    # Fill NaN and alter dtype
    prices.fillna(0, inplace=True)
    prices["Average Price"] = prices["Average Price"].astype(float)
    
    # Statistics
    hub_prices = prices.pivot_table(values="Average Price", index=["Issue Date"], columns=["Price Point Name"])

    hub_stats = {}
    for hub in hub_prices.columns:
        data = hub_prices[hub][hub_prices[hub] != 0]
        if len(data) == 0:
            continue
        
        # Calc day 120 avg and 7 day average
        avg = round(sum(data) / len(data), 2)
        if 0 not in hub_prices[hub][-10:]:
            wk_avg = round(sum(data[-7:]) / 7, 2)
        else:
            wk_avg = None
        
        # Calc DoD difference
        today = round(hub_prices[hub].iloc[-1], 2)
        dod = round(today - hub_prices[hub].iloc[-2], 2)

        # Add to stats dict
        hub_stats[hub] = {"Today":today, "DoD":dod, "Week Avg":wk_avg, "120 Avg":avg}

    # Create df
    hub_stats = pd.DataFrame.from_dict(hub_stats, orient="index")

    # Create additional indicators
    hub_stats["Change DoD"] = hub_stats["DoD"] / (hub_stats["Today"] - hub_stats["DoD"])
    hub_stats["Change DoW"] = (hub_stats["Today"] - hub_stats["Week Avg"]) / hub_stats["Week Avg"]

    # Filter bad values
    hub_stats = hub_stats[hub_stats["Today"] != 0.0]
    hub_stats = hub_stats[hub_stats["Change DoD"] != np.inf]
    
    # Sort and print
    print("Largest positive DoD percentage change:")
    print(hub_stats.sort_values(by=["Change DoD"], ascending=False).head())
    print("Largest positive DoW percentage change:")
    print(hub_stats.sort_values(by=["Change DoW"], ascending=False).head())

    print("Largest negative DoD percentage change:")
    print(hub_stats.sort_values(by=["Change DoD"]).head())
    print("Largest negative DoW percentage change:")
    print(hub_stats.sort_values(by=["Change DoW"]).head())