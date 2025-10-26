import pandas as pd

df = pd.read_csv("data/Gr1_Education_Statistics_Preview.csv", low_memory=False)

kosovo_inds = [
    "Learning Deprivation Gap;PISA 2018 for grade 15Y using MPL Level 2 for reading, Fifth Quintile",
    "Learning Deprivation Gap;PISA 2018 for grade 15Y using MPL Level 2 for science, Rural",
    "Above Proficiency;TIMSS 2019 for grade 4 using MPL Low (400 points) for math, Urban"
]

for ind in kosovo_inds:
    subset = df[df["Indicator name"] == ind]
    countries = subset["Country name"].dropna().unique()
    print(f"\nIndikatori: {ind}")
    print(f"Numri i vendeve me të dhëna: {len(countries)}")
    print("Shembuj të vendeve:", countries[:10])
