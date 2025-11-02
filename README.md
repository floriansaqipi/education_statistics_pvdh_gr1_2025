<table border="0">
 <tr>
    <td><img src="https://upload.wikimedia.org/wikipedia/commons/thumb/e/e1/University_of_Prishtina_logo.svg/1200px-University_of_Prishtina_logo.svg.png" width="150" alt="University Logo" /></td>
    <td>
      <p>Universiteti i Prishtinës</p>
      <p>Fakulteti i Inxhinierisë Elektrike dhe Kompjuterike</p>
      <p>Inxhinieri Kompjuterike dhe Softuerike - Programi Master</p>
      <p>Profesori: Dr. Sc. Mërgim H. HOTI</p>
      <p>Lënda: Përgatitja dhe vizualizimi i të dhënave</p>
    </td>
 </tr>
</table>

# Education Statistics — Data Preparation & Visualization (EdStats / World Bank)

## Overview
This repository contains the coursework for the university subject **Data Preparation and Visualization**.  
It works with the **World Bank EdStats** dataset (CSV), which includes hundreds of education indicators for countries worldwide.  
We apply the full lifecycle of data preparation and analytic engineering to produce **meaningful, presentation‑ready insights** — including custom indices such as:

- **Education Investment Indicator (EII)** and **OUTCOME** composites (learning results), plus an **Efficiency** index that relates outcomes to investment levels (countries/regions/income groups).
- A **gender gap index** and **combined (both genders)** index using education quality indicators across **1970–2023**.
- **Kosovo urban–rural gap analysis** on learning quality (PISA/TIMSS) and **recent trends** for key indicators.

The project explicitly exercises the core topics of the course.
this includes: **data collection, type definition, data quality; integration, aggregation, sampling, cleaning, missing‑value identification and treatment strategy; dimensionality reduction, feature subset selection, feature creation, discretization & binarization, and transformation.**

---

## Technical Details

### Project topology & pipeline conventions
The repository is organized as a **tree of modular stages**. Each top‑level stage is numbered, and sub‑stages use lettered suffixes to convey ordering and dependencies. For example:

```
data_intake/
  1A_source_profiles/
  1B_raw_to_staging/
quality/              # type casting, schema enforcement, basic cleaning
  2A_missing_values/
  2B_outlier_rules/
integration/          # joins: EdStats × country meta (Region, Income group, Lending)
  3A_harmonize_codes/
transformation/       # unpivot, normalization, standardization
  4BA_unpivot_long/
  4BB_latest_snapshot/
  4BD_normalization/
aggregation/          # composites & rollups
  5A_investment_outcome_indices/
  5B_efficiency_country_region_income/
discretization_binarization/
  6A_tertiles_bands/
  6B_threshold_flags/
result_ready/
  7A_country_tables/
  7B_region_income_dash/
```

- **Modular**: every stage is independently runnable and writes its own outputs, so you can work anywhere in the pipeline without running everything end‑to‑end.
- **Reproducible**: intermediate outputs are versioned by stage; downstream steps depend only on the prior stage’s output folder.
- **Flexible**: we use **Python** and **PySpark** for transformation because of their **power, scalability, and expressiveness** on messy, wide, and time‑series‑like datasets.

![Pipeline tree](Pipeline%20example.png)

### How course requirements are fulfilled (at a glance)
- **Data collection / typing / quality**: schema enforcement, unit normalization, percent/decimal cleaning, wide‑to‑long conversion, validity flags.
- **Integration, aggregation, sampling, cleaning, missing values**: integration with country metadata (Region, Income group, Lending category); regex‑driven indicator filtering; windowed latest snapshots; groupBy rollups (country/region/income); explicit NA handling strategies.
- **Dimensionality reduction & feature selection**: standardization to z‑scores per indicator; selection of finance vs learning proxies; composite EII/OUTCOME indices.
- **Feature creation**: efficiency = OUTCOME / shifted(EII); trend slope per year; equity metrics (urban–rural gap); percentile ranks.
- **Discretization & binarization**: quantile bins (tertiles) for EII/OUTCOME/Efficiency; high/low flags for dashboarding.
- **Transformation**: log1p for heavy‑tailed NUMBER units, % to [0–1], standardized z‑scores per indicator group.

---

## How to Run

### Requirements
- **Python 3.10+** (recommended)
- **PySpark** and **NumPy**
- (Optional but recommended) **Java 11+** for local Spark runs

Install dependencies (example using `venv`):
```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install pyspark numpy
```
## Contributors
- [Fatjeta Gashi](https://github.com/fatjetagashi)
- [Florian Saqipi](https://github.com/floriansaqipi)


---

## Acknowledgments
- **Professor / Course Instructor:** _Dr. Sc. Mërgim H. HOTI_ — for guidance on the **Data Preparation & Visualization** methodology and evaluation criteria.
- **World Bank EdStats** team — for providing the open education indicators used in this project.
- Everyone on the project team.
---
