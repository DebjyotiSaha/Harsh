import pandas as pd
from datetime import datetime

#Dataset
df_fpn = pd.read_csv("FPN.csv", parse_dates=["Dates"])
df_offers = pd.read_csv("Offers.csv", parse_dates=["Dates"])
df_bids = pd.read_csv("Bids.csv", parse_dates=["Dates"])
df_sorps = pd.read_csv("SOrPS.csv", parse_dates=["Dates"])

#22:00 - 7:00 hours filter
def filter_night_hours(df, time_column="Date"):
    df["Hour"] = df[time_column].dt.hour
    return df[(df['Hour'] >=22) | (df['Hour'] < 7)]

df_fpn = filter_night_hours(df_fpn)
df_offers = filter_night_hours(df_offers)
df_bids = filter_night_hours(df_bids)
df_sorps = filter_night_hours(df_sorps)

#Offer and Bid Values
offers = df_offers.copy()
offers['OFFERS'] = offers['WAV_Factor1']

bids = df_bids.copy()
bids['BIDS'] = bids['WAV_Factor1'] / bids['WAV_Factor2']

#Joining the data
df_all = df_fpn.merge(offers[['Date', 'OFFERS']], on='Date', how='inner') \
    .merge(bids[['Date', 'BIDS']], on='Date', how='inner')

#Synch Calculation
df_all["Synch"] = df_all['SEL'] * (df_all['OFFERS'] - df_all['BIDS'])

# --- MVAr Calculation ---
v_mw_max = 1000
v_mw_min = 500
v_mvar_max = 300
v_mvar_min = 100

# Line equation
a = (v_mvar_max - v_mvar_min) / (v_mw_min - v_mw_max)
b = v_mvar_max - (a * v_mw_min)

# Estimate reactive absorption (MVAr)
def estimate_mvar(final_physical, stable_limit, error=0.01):
    if final_physical >= stable_limit * (1 - error):
        return (final_physical * a + b)
    else:
        return 0

df_all['MVAr'] = df_all.apply(
    lambda row: estimate_mvar(row['SEL'], row['SEL']), axis=1
)

# --- Merge SOrPS values ---
df_all = df_all.merge(df_sorps[['Date', 'ORPS_GBP_PER_MVArh']], on='Date', how='inner')

# --- SOrPS Calculation: MVAr * 0.5 * SOrPS ---
df_all['SOrPS'] = df_all['MVAr'] * 0.5 * df_all['ORPS_GBP_PER_MVArh']

# --- Final Output ---
print(df_all[['Date', 'Synchronisation', 'SOrPS']])

# --- Load Greenlink data ---
df_greenlink = pd.read_csv("Greenlink_Status.csv", parse_dates=["Date"])
df_greenlink = filter_night_hours(df_greenlink)

# --- Displacement Logic ---
def check_displacement(row):
    return (row['Direction'] == 'Export') and (row['VCC_Status'] == 'Open')

df_greenlink['Displacement_Possible'] = df_greenlink.apply(check_displacement, axis=1)

# Merge into master dataframe
df_all = df_all.merge(df_greenlink[['Date', 'Displacement_Possible']], on='Date', how='left')


# --- Load PEMB data ---
df_pemb = pd.read_csv("PEMB_Status.csv", parse_dates=["Date"])
df_pemb = filter_night_hours(df_pemb)

# Merge into main
df_all = df_all.merge(df_pemb[['Date', 'PEMB_Total', 'PEMB_Displaced']], on='Date', how='left')

# Apply logic
def fpn_condition(row):
    if row['SEL'] > 0:
        return "No Savings"
    elif row['SEL'] == 0:
        return f"{row['PEMB_Displaced']} displaced of {row['PEMB_Total']}"
    else:
        return "Unknown"

df_all['PEMB_Status'] = df_all.apply(fpn_condition, axis=1)


print(df_all[['Date', 'Synchronisation', 'SOrPS', 'Displacement_Possible', 'PEMB_Status']])
