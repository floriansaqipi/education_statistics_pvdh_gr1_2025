import os
import pandas as pd

df = pd.read_csv("data/Gr1_Education_Statistics_Preview.csv", low_memory=False)

kosovo_df = df[df["Country name"].str.contains("Kosovo", case=False, na=False)].copy()

year_cols = [c for c in kosovo_df.columns if c.startswith("YR") and c[2:].isdigit()]

id_vars = [c for c in kosovo_df.columns if c not in year_cols]
kosovo_long = kosovo_df.melt(
    id_vars=id_vars,
    value_vars=year_cols,
    var_name="Year",
    value_name="Value"
)
kosovo_long["Year"] = kosovo_long["Year"].str.replace("YR", "", regex=False)
kosovo_long["Year"] = pd.to_numeric(kosovo_long["Year"], errors="coerce")

kosovo_long = kosovo_long.dropna(subset=["Value"])

os.makedirs("output", exist_ok=True)
out_path = "output/kosovo_all_data.csv"
kosovo_long.to_csv(out_path, index=False, encoding="utf-8")


print("Të dhënat për Kosovën u ruajtën në:", out_path)
print("Numri total i rreshtave:", len(kosovo_long))
print("Numri i indikatorëve unikë:", kosovo_long["Indicator name"].nunique())
print("\nShembull i disa rreshtave:")
print(kosovo_long.head(10))
