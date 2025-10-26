import os
import re
import pandas as pd

TOP_N = 10
MIN_COUNTRIES = 40
MIN_YEARS = 20

SRC_PATH = "data/Gr1_Education_Statistics_Preview.csv"
COUNTS_PATH = "data/indicator_value_counts.csv"
OUT_DIR = "data/integration"

os.makedirs(OUT_DIR, exist_ok=True)

df = pd.read_csv(SRC_PATH, low_memory=False)

year_cols = [c for c in df.columns if c.startswith("YR") and c[2:].isdigit()]
for req in ["Indicator name", "Country name"]:
    if req not in df.columns:
        raise KeyError(f"Mungon kolona: {req}")


if os.path.exists(COUNTS_PATH):
    counts = pd.read_csv(COUNTS_PATH)
else:
    counts = (
        df.groupby("Indicator name")[year_cols]
        .apply(lambda x: x.notna().sum().sum())
        .reset_index(name="non_null_values")
        .sort_values("non_null_values", ascending=False)
    )
    os.makedirs(os.path.dirname(COUNTS_PATH), exist_ok=True)
    counts.to_csv(COUNTS_PATH, index=False)

df_long_all = df.melt(
    id_vars=[c for c in ["Indicator name", "Country name", "UNIT_MEASURE", "UNIT_TYPE", "economy"] if c in df.columns],
    value_vars=year_cols,
    var_name="Year",
    value_name="Value"
)
df_long_all["Year"] = df_long_all["Year"].str.replace("YR", "", regex=False).astype(int)

quality = (
    df_long_all.dropna(subset=["Value"])
    .groupby("Indicator name")
    .agg(
        non_null_values=("Value", "size"),
        countries_covered=("Country name", "nunique"),
        years_covered=("Year", "nunique")
    )
    .reset_index()
)
quality = quality.sort_values("non_null_values", ascending=False)


top = (
    quality.sort_values("non_null_values", ascending=False)
    .head(TOP_N)
    .copy()
)
top = top[
    (top["countries_covered"] >= MIN_COUNTRIES) &
    (top["years_covered"] >= MIN_YEARS)
].copy()


if top.empty:
    top = quality.head(TOP_N).copy()

top_names = top["Indicator name"].tolist()
pd.DataFrame(top_names, columns=["Indicator name"]).to_csv(
    os.path.join(OUT_DIR, "top_indicators_selected.csv"), index=False, encoding="utf-8"
)

df_sel = df[df["Indicator name"].isin(top_names)].copy()


keep_cols = [c for c in ["economy", "Country name", "Indicator name", "UNIT_MEASURE", "UNIT_TYPE"] if c in df_sel.columns]
df_long = df_sel.melt(
    id_vars=keep_cols,
    value_vars=year_cols,
    var_name="Year",
    value_name="Value"
).dropna(subset=["Value"])

df_long["Year"] = df_long["Year"].str.replace("YR", "", regex=False).astype(int)

dict_cols = ["Indicator name"]
for extra in ["UNIT_MEASURE", "UNIT_TYPE"]:
    if extra in df_long.columns:
        dict_cols.append(extra)
data_dict = (
    df_long[dict_cols]
    .drop_duplicates()
    .merge(
        quality[["Indicator name", "non_null_values", "countries_covered", "years_covered"]],
        on="Indicator name", how="left"
    )
)
data_dict_path = os.path.join(OUT_DIR, "data_dictionary.csv")
data_dict.to_csv(data_dict_path, index=False, encoding="utf-8")


index_cols = [c for c in ["economy", "Country name", "Year"] if c in df_long.columns]
df_wide = (
    df_long
    .pivot_table(index=index_cols, columns="Indicator name", values="Value", aggfunc="mean")
    .reset_index()
)

def slugify(s):
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")
    s = re.sub(r"_+", "_", s)
    return s[:80]

rename_map = {c: slugify(c) for c in df_wide.columns}
df_wide = df_wide.rename(columns=rename_map)

# Ruajtje
panel_long_path = os.path.join(OUT_DIR, "panel_long.csv")
panel_wide_path = os.path.join(OUT_DIR, "panel_wide.csv")
quality_path = os.path.join(OUT_DIR, "indicator_quality_report.csv")

df_long.to_csv(panel_long_path, index=False, encoding="utf-8")
df_wide.to_csv(panel_wide_path, index=False, encoding="utf-8")
quality.to_csv(quality_path, index=False, encoding="utf-8")

print("Top indikatorët:", os.path.join(OUT_DIR, "top_indicators_selected.csv"))
print("Panel (long):", panel_long_path)
print("Panel (wide):", panel_wide_path)
print("Raport cilësie:", quality_path)
print("Data dictionary:", data_dict_path)

print("\nShembull i top indikatorëve:")
print(pd.read_csv(os.path.join(OUT_DIR, "top_indicators_selected.csv")).to_string(index=False))

print("\nShembull (long):")
print(df_long.head(8).to_string(index=False))

print("\nShembull (wide):")
print(df_wide.head(5).to_string(index=False))
