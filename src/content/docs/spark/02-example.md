---
title: Spark Examples
description: Worked Spark Example
lang: en
---

## Getting Started with Azure Databricks - A Census Data Example

This notebook is a simple example of working with data in Azure Databricks.

If you are reading this on the wiki, you can find the working Notebook in the Azure Databricks environment at the following path: [/Shared/Getting Started with Azure Databricks - A Census Data Example](https://adb-5195694350474952.12.azuredatabricks.net/?o=5195694350474952#notebook/3876325875947739)

### Explore the ADLS folders

You can explore filesystems directly through Notebooks by using the [`dbutils.fs` client](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils).

Below we exaplore the CSVs for the night population census:

```python
display(dbutils.fs.ls("/"))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>path</th><th>name</th><th>size</th></tr></thead><tbody><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/data/</td><td>data/</td><td>0</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/</td><td>tables/</td><td>0</td></tr></tbody></table></div>

```python
display(dbutils.fs.ls("abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/"))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>path</th><th>name</th><th>size</th></tr></thead><tbody><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/data/</td><td>data/</td><td>0</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/</td><td>tables/</td><td>0</td></tr></tbody></table></div>

```python
display(dbutils.fs.ls("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz"))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>path</th><th>name</th><th>size</th></tr></thead><tbody><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/Data8317.csv</td><td>Data8317.csv</td><td>857219761</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupAge8317.csv</td><td>DimenLookupAge8317.csv</td><td>2720</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupArea8317.csv</td><td>DimenLookupArea8317.csv</td><td>65400</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupEthnic8317.csv</td><td>DimenLookupEthnic8317.csv</td><td>272</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupSex8317.csv</td><td>DimenLookupSex8317.csv</td><td>74</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupYear8317.csv</td><td>DimenLookupYear8317.csv</td><td>67</td></tr></tbody></table></div>

As you can see above, the ADLS Gen2 container for the project (`abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/`) is set as the default filesystem for clusters associated with that project (in this case `sandbox`).

### Read in the CSVs and explore the data

We can read the source CSVs into Spark as DataFrames and explore the data.

We use the [DataFrame API](https://spark.apache.org/docs/3.0.0/sql-getting-started.html) to transform the DataFrames.

```python
df = spark.read.csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/Data8317.csv")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
display(df.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>_c0</th><th>_c1</th><th>_c2</th><th>_c3</th><th>_c4</th><th>_c5</th></tr></thead><tbody><tr><td>Year</td><td>Age</td><td>Ethnic</td><td>Sex</td><td>Area</td><td>count</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>01</td><td>807</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>02</td><td>5109</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>03</td><td>2262</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>04</td><td>1359</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>05</td><td>180</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>06</td><td>741</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>07</td><td>633</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>08</td><td>1206</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>09</td><td>2184</td></tr></tbody></table></div>

The datatypes and headers look incorrect: all the datatypes are strings and the headers have not been read. We can fix this by passing in [reader options](https://spark.apache.org/docs/2.3.0/sql-programming-guide.html#manually-specifying-options).

```python
df = spark.read.option("header", True).option("inferSchema", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/Data8317.csv")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
display(df.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>_c0</th><th>_c1</th><th>_c2</th><th>_c3</th><th>_c4</th><th>_c5</th></tr></thead><tbody><tr><td>Year</td><td>Age</td><td>Ethnic</td><td>Sex</td><td>Area</td><td>count</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>01</td><td>807</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>02</td><td>5109</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>03</td><td>2262</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>04</td><td>1359</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>05</td><td>180</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>06</td><td>741</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>07</td><td>633</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>08</td><td>1206</td></tr><tr><td>2018</td><td>000</td><td>1</td><td>1</td><td>09</td><td>2184</td></tr></tbody></table></div>

```python
df.count()
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">Out[11]: 34959673</div>

Let's take a look at a couple of the dimension tables too.

```python
df = spark.read.option("header", True).option("inferSchema", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupAge8317.csv")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
display(df.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Code</th><th>Description</th><th>SortOrder</th></tr></thead><tbody><tr><td>999999</td><td>Total people - age group</td><td>1</td></tr><tr><td>888</td><td>Median age</td><td>2</td></tr><tr><td>1</td><td>Under 15 years</td><td>3</td></tr><tr><td>2</td><td>15-29 years</td><td>4</td></tr><tr><td>3</td><td>30-64 years</td><td>5</td></tr><tr><td>4</td><td>65 years and over</td><td>6</td></tr><tr><td>1</td><td>0-4 years</td><td>7</td></tr><tr><td>2</td><td>5-9 years</td><td>8</td></tr><tr><td>3</td><td>10-14 years</td><td>9</td></tr><tr><td>4</td><td>15-19 years</td><td>10</td></tr></tbody></table></div>

```python
df = spark.read.option("header", True).option("inferSchema", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupEthnic8317.csv")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
display(df.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Code</th><th>Description</th><th>SortOrder</th></tr></thead><tbody><tr><td>9999</td><td>Total people</td><td>1</td></tr><tr><td>1</td><td>European</td><td>2</td></tr><tr><td>2</td><td>Maori</td><td>3</td></tr><tr><td>3</td><td>Pacific Peoples</td><td>4</td></tr><tr><td>4</td><td>Asian</td><td>5</td></tr><tr><td>5</td><td>Middle Eastern/Latin American/African</td><td>6</td></tr><tr><td>6</td><td>Other ethnicity</td><td>7</td></tr><tr><td>61</td><td>New Zealander</td><td>10</td></tr><tr><td>69</td><td>Other ethnicity nec</td><td>11</td></tr><tr><td>77</td><td>Total people stated</td><td>8</td></tr></tbody></table></div>

Observation: the dimension tables seem to have a consistent schema.

### Denormalise the source tables

Since we are using Apache Spark we want to limit joins the number of joins/data transfer between nodes. Filters and aggregations suit the architecture better, and data will be stored in columnar-compressed files therefore it would make sense to denormalise the data.

Let's join all the data into one large DataFrame:

```python
from pyspark.sql.functions import col

denorm_df = spark.read.option("header", True).option("inferSchema", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/Data8317.csv")
for dim in ["Age", "Area", "Ethnic", "Sex", "Year"]:
  dim_df = spark.read.option("header", True).option("inferSchema", True).csv(f"/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookup{dim}8317.csv")
  denorm_df = denorm_df.join(dim_df, col(dim) == col("Code")).drop("Code", dim).withColumnRenamed("Description", dim).withColumnRenamed("SortOrder", f"{dim}SortOrder")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
display(denorm_df.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>count</th><th>Age</th><th>AgeSortOrder</th><th>Area</th><th>AreaSortOrder</th><th>Ethnic</th><th>EthnicSortOrder</th><th>Sex</th><th>SexSortOrder</th><th>Year</th><th>YearSortOrder</th></tr></thead><tbody><tr><td>807</td><td>Less than one year</td><td>28</td><td>Northland Region</td><td>4</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>5109</td><td>Less than one year</td><td>28</td><td>Auckland Region</td><td>5</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2262</td><td>Less than one year</td><td>28</td><td>Waikato Region</td><td>6</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>1359</td><td>Less than one year</td><td>28</td><td>Bay of Plenty Region</td><td>7</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>180</td><td>Less than one year</td><td>28</td><td>Gisborne Region</td><td>8</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>741</td><td>Less than one year</td><td>28</td><td>Hawke's Bay Region</td><td>9</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>633</td><td>Less than one year</td><td>28</td><td>Taranaki Region</td><td>10</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>1206</td><td>Less than one year</td><td>28</td><td>Manawatu-Wanganui Region</td><td>11</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2184</td><td>Less than one year</td><td>28</td><td>Wellington Region</td><td>12</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>177</td><td>Less than one year</td><td>28</td><td>West Coast Region</td><td>16</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr></tbody></table></div>

```python
denorm_df.count()
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">Out[18]: 48585735</div>

### Investigate duplicates 🕵️‍♀️

> Pre-join count: 34959673

> Post-join count: 48585735

The counts look incorrect: the dimension joins are only lookups and should not produce additional rows.

Let's look into why this has happened.

Hypothesis: the code column shouldn't be inferred as an integer column.

With schema inference:

```python
df = spark.read.option("header", True).option("inferSchema", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupAge8317.csv")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
display(df.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Code</th><th>Description</th><th>SortOrder</th></tr></thead><tbody><tr><td>999999</td><td>Total people - age group</td><td>1</td></tr><tr><td>888</td><td>Median age</td><td>2</td></tr><tr><td>1</td><td>Under 15 years</td><td>3</td></tr><tr><td>2</td><td>15-29 years</td><td>4</td></tr><tr><td>3</td><td>30-64 years</td><td>5</td></tr><tr><td>4</td><td>65 years and over</td><td>6</td></tr><tr><td>1</td><td>0-4 years</td><td>7</td></tr><tr><td>2</td><td>5-9 years</td><td>8</td></tr><tr><td>3</td><td>10-14 years</td><td>9</td></tr><tr><td>4</td><td>15-19 years</td><td>10</td></tr></tbody></table></div>

Without schema inference:

```python
df = spark.read.option("header", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupAge8317.csv")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
display(df.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Code</th><th>Description</th><th>SortOrder</th></tr></thead><tbody><tr><td>999999</td><td>Total people - age group</td><td>1</td></tr><tr><td>888</td><td>Median age</td><td>2</td></tr><tr><td>1</td><td>Under 15 years</td><td>3</td></tr><tr><td>2</td><td>15-29 years</td><td>4</td></tr><tr><td>3</td><td>30-64 years</td><td>5</td></tr><tr><td>4</td><td>65 years and over</td><td>6</td></tr><tr><td>01</td><td>0-4 years</td><td>7</td></tr><tr><td>02</td><td>5-9 years</td><td>8</td></tr><tr><td>03</td><td>10-14 years</td><td>9</td></tr><tr><td>04</td><td>15-19 years</td><td>10</td></tr></tbody></table></div>

Schema inference shot us in the foot! 🦶🔫

Try again without infering any datatypes (we can manually cast later!):

```python
from pyspark.sql.functions import col

denorm_df = spark.read.option("header", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/Data8317.csv")
for dim in ["Age", "Area", "Ethnic", "Sex", "Year"]:
  dim_df = spark.read.option("header", True).csv(f"/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookup{dim}8317.csv")
  denorm_df = denorm_df.join(dim_df, col(dim) == col("Code")).drop("Code", dim).withColumnRenamed("Description", dim).withColumnRenamed("SortOrder", f"{dim}SortOrder")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
denorm_df.count()
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">Out[24]: 34885323</div>

> Pre-join count: 34959673

> Post-join count: 34885323

Closer, but it looks like we lost a few rows this time.

Let's try a left join:

```python
from pyspark.sql.functions import col

denorm_df = spark.read.option("header", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/Data8317.csv")
for dim in ["Age", "Area", "Ethnic", "Sex", "Year"]:
  dim_df = spark.read.option("header", True).csv(f"/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookup{dim}8317.csv")
  denorm_df = denorm_df.join(dim_df, col(dim) == col("Code"), how="left").drop("Code", dim).withColumnRenamed("Description", dim).withColumnRenamed("SortOrder", f"{dim}SortOrder")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
denorm_df.count()
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">Out[26]: 34959672</div>

Bingo! The counts match. However, this implies something doesn't join to it's dimension lookup.

Let's hunt for nulls:

```python
display(denorm_df.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>count</th><th>Age</th><th>AgeSortOrder</th><th>Area</th><th>AreaSortOrder</th><th>Ethnic</th><th>EthnicSortOrder</th><th>Sex</th><th>SexSortOrder</th><th>Year</th><th>YearSortOrder</th></tr></thead><tbody><tr><td>807</td><td>Less than one year</td><td>28</td><td>Northland Region</td><td>4</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>5109</td><td>Less than one year</td><td>28</td><td>Auckland Region</td><td>5</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2262</td><td>Less than one year</td><td>28</td><td>Waikato Region</td><td>6</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>1359</td><td>Less than one year</td><td>28</td><td>Bay of Plenty Region</td><td>7</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>180</td><td>Less than one year</td><td>28</td><td>Gisborne Region</td><td>8</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>741</td><td>Less than one year</td><td>28</td><td>Hawke's Bay Region</td><td>9</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>633</td><td>Less than one year</td><td>28</td><td>Taranaki Region</td><td>10</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>1206</td><td>Less than one year</td><td>28</td><td>Manawatu-Wanganui Region</td><td>11</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2184</td><td>Less than one year</td><td>28</td><td>Wellington Region</td><td>12</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>177</td><td>Less than one year</td><td>28</td><td>West Coast Region</td><td>16</td><td>European</td><td>2</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr></tbody></table></div>

```python
denorm_df_nulls = denorm_df.filter(col("Age").isNull() | col("Area").isNull() | col("Ethnic").isNull() | col("Sex").isNull() | col("Year").isNull())
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
denorm_df_nulls.count()
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">Out[29]: 74349</div>

```python
display(denorm_df_nulls.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>count</th><th>Age</th><th>AgeSortOrder</th><th>Area</th><th>AreaSortOrder</th><th>Ethnic</th><th>EthnicSortOrder</th><th>Sex</th><th>SexSortOrder</th><th>Year</th><th>YearSortOrder</th></tr></thead><tbody><tr><td>50</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>48.5</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>49.2</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>29.5</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>42.3</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>36</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>21.2</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr><tr><td>24.9</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr><tr><td>23.3</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr><tr><td>26.6</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr></tbody></table></div>

```python
denorm_df_nulls.filter(col("Age") == "Median age").count()
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">Out[31]: 74349</div>

All the duplicates come from the median age category.

We should take some time to understand our data!

```python
display(spark.read.option("header", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupAge8317.csv").limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Code</th><th>Description</th><th>SortOrder</th></tr></thead><tbody><tr><td>999999</td><td>Total people - age group</td><td>1</td></tr><tr><td>888</td><td>Median age</td><td>2</td></tr><tr><td>1</td><td>Under 15 years</td><td>3</td></tr><tr><td>2</td><td>15-29 years</td><td>4</td></tr><tr><td>3</td><td>30-64 years</td><td>5</td></tr><tr><td>4</td><td>65 years and over</td><td>6</td></tr><tr><td>01</td><td>0-4 years</td><td>7</td></tr><tr><td>02</td><td>5-9 years</td><td>8</td></tr><tr><td>03</td><td>10-14 years</td><td>9</td></tr><tr><td>04</td><td>15-19 years</td><td>10</td></tr></tbody></table></div>

Let's filter the fact table by the top two codes as they look odd:

```python
df = spark.read.option("header", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/Data8317.csv").filter(col("Age").isin(["999999", "888"]))
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
display(df.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Year</th><th>Age</th><th>Ethnic</th><th>Sex</th><th>Area</th><th>count</th></tr></thead><tbody><tr><td>2018</td><td>999999</td><td>1</td><td>1</td><td>01</td><td>65307</td></tr><tr><td>2018</td><td>999999</td><td>1</td><td>1</td><td>02</td><td>418347</td></tr><tr><td>2018</td><td>999999</td><td>1</td><td>1</td><td>03</td><td>169422</td></tr><tr><td>2018</td><td>999999</td><td>1</td><td>1</td><td>04</td><td>110733</td></tr><tr><td>2018</td><td>999999</td><td>1</td><td>1</td><td>05</td><td>13566</td></tr><tr><td>2018</td><td>999999</td><td>1</td><td>1</td><td>06</td><td>60591</td></tr><tr><td>2018</td><td>999999</td><td>1</td><td>1</td><td>07</td><td>49086</td></tr><tr><td>2018</td><td>999999</td><td>1</td><td>1</td><td>08</td><td>92655</td></tr><tr><td>2018</td><td>999999</td><td>1</td><td>1</td><td>09</td><td>186054</td></tr><tr><td>2018</td><td>999999</td><td>1</td><td>1</td><td>12</td><td>15735</td></tr></tbody></table></div>

```python
display(df.filter(col("Age") == "888").limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Year</th><th>Age</th><th>Ethnic</th><th>Sex</th><th>Area</th><th>count</th></tr></thead><tbody><tr><td>2018</td><td>888</td><td>1</td><td>1</td><td>01</td><td>46.4</td></tr><tr><td>2018</td><td>888</td><td>1</td><td>1</td><td>02</td><td>38.5</td></tr><tr><td>2018</td><td>888</td><td>1</td><td>1</td><td>03</td><td>40</td></tr><tr><td>2018</td><td>888</td><td>1</td><td>1</td><td>04</td><td>43.4</td></tr><tr><td>2018</td><td>888</td><td>1</td><td>1</td><td>05</td><td>41.4</td></tr><tr><td>2018</td><td>888</td><td>1</td><td>1</td><td>06</td><td>43.6</td></tr><tr><td>2018</td><td>888</td><td>1</td><td>1</td><td>07</td><td>40.6</td></tr><tr><td>2018</td><td>888</td><td>1</td><td>1</td><td>08</td><td>40.8</td></tr><tr><td>2018</td><td>888</td><td>1</td><td>1</td><td>09</td><td>38.4</td></tr><tr><td>2018</td><td>888</td><td>1</td><td>1</td><td>12</td><td>47.7</td></tr></tbody></table></div>

It's getting a little hard to trace columns and codes, so let's denormalise whilst retaining the code columns:

```python
from pyspark.sql.functions import col

denorm_df = spark.read.option("header", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/Data8317.csv")
for dim in ["Age", "Area", "Ethnic", "Sex", "Year"]:
  dim_df = spark.read.option("header", True).csv(f"/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookup{dim}8317.csv")
  denorm_df = denorm_df.join(dim_df, col(dim) == col("Code"), how="left").drop("Code").withColumnRenamed(dim, f"{dim}Code").withColumnRenamed("Description", dim).withColumnRenamed("SortOrder", f"{dim}SortOrder")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
denorm_df_nulls = denorm_df.filter(col("Age").isNull() | col("Area").isNull() | col("Ethnic").isNull() | col("Sex").isNull() | col("Year").isNull())
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

```python
display(denorm_df_nulls.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>YearCode</th><th>AgeCode</th><th>EthnicCode</th><th>SexCode</th><th>AreaCode</th><th>count</th><th>Age</th><th>AgeSortOrder</th><th>Area</th><th>AreaSortOrder</th><th>Ethnic</th><th>EthnicSortOrder</th><th>Sex</th><th>SexSortOrder</th><th>Year</th><th>YearSortOrder</th></tr></thead><tbody><tr><td>2018</td><td>888</td><td>100100</td><td>1</td><td>1</td><td>50</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>1</td><td>2</td><td>48.5</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>1</td><td>9</td><td>49.2</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>2</td><td>1</td><td>29.5</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>2</td><td>2</td><td>42.3</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>2</td><td>9</td><td>36</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>3</td><td>1</td><td>21.2</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>3</td><td>2</td><td>24.9</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>3</td><td>9</td><td>23.3</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>4</td><td>1</td><td>26.6</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr></tbody></table></div>

The area code doesn't match.

Let's dig deeper:

```python
display(spark.read.option("header", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupArea8317.csv").filter(col("Code").isin(["1", "01"])))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Code</th><th>Description</th><th>SortOrder</th></tr></thead><tbody><tr><td>01</td><td>Northland Region</td><td>4</td></tr></tbody></table></div>

Seems like some of the area codes don't lookup correctly.

Let's also look at sex:

```python
display(denorm_df_nulls.limit(10))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>YearCode</th><th>AgeCode</th><th>EthnicCode</th><th>SexCode</th><th>AreaCode</th><th>count</th><th>Age</th><th>AgeSortOrder</th><th>Area</th><th>AreaSortOrder</th><th>Ethnic</th><th>EthnicSortOrder</th><th>Sex</th><th>SexSortOrder</th><th>Year</th><th>YearSortOrder</th></tr></thead><tbody><tr><td>2018</td><td>888</td><td>100100</td><td>1</td><td>1</td><td>50</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>1</td><td>2</td><td>48.5</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>1</td><td>9</td><td>49.2</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>2</td><td>1</td><td>29.5</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>2</td><td>2</td><td>42.3</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>2</td><td>9</td><td>36</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>3</td><td>1</td><td>21.2</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>3</td><td>2</td><td>24.9</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>3</td><td>9</td><td>23.3</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>888</td><td>100100</td><td>4</td><td>1</td><td>26.6</td><td>Median age</td><td>2</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>2018</td><td>3</td></tr></tbody></table></div>

```python
display(spark.read.option("header", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookupSex8317.csv"))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>Code</th><th>Description</th><th>SortOrder</th></tr></thead><tbody><tr><td>9</td><td>Total people - sex</td><td>1</td></tr><tr><td>1</td><td>Male</td><td>2</td></tr><tr><td>2</td><td>Female</td><td>3</td></tr></tbody></table></div>

Conclusion: the data looks a little odd and needs investigating further. For now, let's continue with the denormalisation as we can fix these issues later by keeping the raw CSVs.

### Creating output files, Hive databases and tables

When writing out DataFrames, we can write them out in [various formats](https://spark.apache.org/docs/3.0.0/sql-data-sources.html), and optionally add a Hive table over these files.

Hive tables are 'virtual' SQL tables over data stored (in this case stored on ADLS).
We can use either:

- External tables: create tables over existing data
- Hive-managed tables: create tables and data at the same time

Both options produce the same end, and are only subtly different.

When we create Hive tables, we are really writing out the DataFrame to ADLS and adding a schema and file path reference to Hive.

Let's create a Hive database:

```sql
%sql
create database if not exists sandbox;
use sandbox;
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr></tr></thead><tbody></tbody></table></div>

Now, let's create our final DataFrame we would like to write out:

```python
denorm_df = spark.read.option("header", True).csv("/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/Data8317.csv")
for dim in ["Age", "Area", "Ethnic", "Sex", "Year"]:
  dim_df = spark.read.option("header", True).csv(f"/data/raw/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/DimenLookup{dim}8317.csv")
  denorm_df = denorm_df.join(dim_df, col(dim) == col("Code"), how="left").drop("Code").withColumnRenamed(dim, f"{dim}Code").withColumnRenamed("Description", dim).withColumnRenamed("SortOrder", f"{dim}SortOrder")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

We can write the files out directly as Parquet (with no Hive table):

```python
denorm_df.write.mode("overwrite").parquet("/data/derived/stats_nz_census/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz_denorm/")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

Or we can write the files out (by default in Parquet) and create a Hive table:

```python
denorm_df.write.mode("overwrite").saveAsTable("sandbox.age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

We can now query the Hive table using SQL:

```sql
%sql
select * from sandbox.age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz
limit 10
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>YearCode</th><th>AgeCode</th><th>EthnicCode</th><th>SexCode</th><th>AreaCode</th><th>count</th><th>Age</th><th>AgeSortOrder</th><th>Area</th><th>AreaSortOrder</th><th>Ethnic</th><th>EthnicSortOrder</th><th>Sex</th><th>SexSortOrder</th><th>Year</th><th>YearSortOrder</th></tr></thead><tbody><tr><td>2018</td><td>110</td><td>61</td><td>9</td><td>120300</td><td>0</td><td>110 years</td><td>138</td><td>West Harbour Clearwater Cove</td><td>317</td><td>New Zealander</td><td>10</td><td>Total people - sex</td><td>1</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>110</td><td>69</td><td>1</td><td>120300</td><td>..C</td><td>110 years</td><td>138</td><td>West Harbour Clearwater Cove</td><td>317</td><td>Other ethnicity nec</td><td>11</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>110</td><td>69</td><td>2</td><td>120300</td><td>..C</td><td>110 years</td><td>138</td><td>West Harbour Clearwater Cove</td><td>317</td><td>Other ethnicity nec</td><td>11</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>110</td><td>69</td><td>9</td><td>120300</td><td>0</td><td>110 years</td><td>138</td><td>West Harbour Clearwater Cove</td><td>317</td><td>Other ethnicity nec</td><td>11</td><td>Total people - sex</td><td>1</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>110</td><td>61</td><td>1</td><td>120400</td><td>..C</td><td>110 years</td><td>138</td><td>Unsworth Heights East</td><td>318</td><td>New Zealander</td><td>10</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>110</td><td>61</td><td>2</td><td>120400</td><td>..C</td><td>110 years</td><td>138</td><td>Unsworth Heights East</td><td>318</td><td>New Zealander</td><td>10</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>110</td><td>61</td><td>9</td><td>120400</td><td>0</td><td>110 years</td><td>138</td><td>Unsworth Heights East</td><td>318</td><td>New Zealander</td><td>10</td><td>Total people - sex</td><td>1</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>110</td><td>69</td><td>1</td><td>120400</td><td>..C</td><td>110 years</td><td>138</td><td>Unsworth Heights East</td><td>318</td><td>Other ethnicity nec</td><td>11</td><td>Male</td><td>2</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>110</td><td>69</td><td>2</td><td>120400</td><td>..C</td><td>110 years</td><td>138</td><td>Unsworth Heights East</td><td>318</td><td>Other ethnicity nec</td><td>11</td><td>Female</td><td>3</td><td>2018</td><td>3</td></tr><tr><td>2018</td><td>110</td><td>69</td><td>9</td><td>120400</td><td>0</td><td>110 years</td><td>138</td><td>Unsworth Heights East</td><td>318</td><td>Other ethnicity nec</td><td>11</td><td>Total people - sex</td><td>1</td><td>2018</td><td>3</td></tr></tbody></table></div>

And we have three ways of opening the Hive table as a DataFrame:

```python
df_s = spark.sql("select * from sandbox.age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz")
df_h = spark.read.table("sandbox.age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz")
df_p = spark.read.parquet("/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/")
```

<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>

The underlying Hive-backed table metadata and data files look like the following:

```sql
%sql
describe formatted sandbox.age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz;
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>col_name</th><th>data_type</th><th>comment</th></tr></thead><tbody><tr><td>YearCode</td><td>string</td><td>null</td></tr><tr><td>AgeCode</td><td>string</td><td>null</td></tr><tr><td>EthnicCode</td><td>string</td><td>null</td></tr><tr><td>SexCode</td><td>string</td><td>null</td></tr><tr><td>AreaCode</td><td>string</td><td>null</td></tr><tr><td>count</td><td>string</td><td>null</td></tr><tr><td>Age</td><td>string</td><td>null</td></tr><tr><td>AgeSortOrder</td><td>string</td><td>null</td></tr><tr><td>Area</td><td>string</td><td>null</td></tr><tr><td>AreaSortOrder</td><td>string</td><td>null</td></tr><tr><td>Ethnic</td><td>string</td><td>null</td></tr><tr><td>EthnicSortOrder</td><td>string</td><td>null</td></tr><tr><td>Sex</td><td>string</td><td>null</td></tr><tr><td>SexSortOrder</td><td>string</td><td>null</td></tr><tr><td>Year</td><td>string</td><td>null</td></tr><tr><td>YearSortOrder</td><td>string</td><td>null</td></tr><tr><td></td><td></td><td></td></tr><tr><td># Detailed Table Information</td><td></td><td></td></tr><tr><td>Database</td><td>sandbox</td><td></td></tr><tr><td>Table</td><td>age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz</td><td></td></tr><tr><td>Owner</td><td>root</td><td></td></tr><tr><td>Created Time</td><td>Thu Nov 12 02:32:15 UTC 2020</td><td></td></tr><tr><td>Last Access</td><td>UNKNOWN</td><td></td></tr><tr><td>Created By</td><td>Spark 3.0.0</td><td></td></tr><tr><td>Type</td><td>MANAGED</td><td></td></tr><tr><td>Provider</td><td>parquet</td><td></td></tr><tr><td>Location</td><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz</td><td></td></tr><tr><td>Serde Library</td><td>org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe</td><td></td></tr><tr><td>InputFormat</td><td>org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat</td><td></td></tr><tr><td>OutputFormat</td><td>org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat</td><td></td></tr></tbody></table></div>

```python
display(dbutils.fs.ls("/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/"))
```

<style scoped>
  .table-result-container {
    max-height: 300px;
    overflow: auto;
  }
  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
  }
  th, td {
    padding: 5px;
  }
  th {
    text-align: left;
  }
</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>path</th><th>name</th><th>size</th></tr></thead><tbody><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/_SUCCESS</td><td>_SUCCESS</td><td>0</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/_committed_5626976569612752113</td><td>_committed_5626976569612752113</td><td>724</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/_started_5626976569612752113</td><td>_started_5626976569612752113</td><td>0</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/part-00000-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-247-1-c000.snappy.parquet</td><td>part-00000-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-247-1-c000.snappy.parquet</td><td>5614085</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/part-00001-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-248-1-c000.snappy.parquet</td><td>part-00001-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-248-1-c000.snappy.parquet</td><td>5036655</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/part-00002-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-249-1-c000.snappy.parquet</td><td>part-00002-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-249-1-c000.snappy.parquet</td><td>9889226</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/part-00003-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-250-1-c000.snappy.parquet</td><td>part-00003-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-250-1-c000.snappy.parquet</td><td>8736544</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/part-00004-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-251-1-c000.snappy.parquet</td><td>part-00004-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-251-1-c000.snappy.parquet</td><td>9473689</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/part-00005-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-252-1-c000.snappy.parquet</td><td>part-00005-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-252-1-c000.snappy.parquet</td><td>9039443</td></tr><tr><td>abfss://sandbox@aueprddlsnzlh001.dfs.core.windows.net/tables/sandbox.db/age_and_sex_by_ethnic_group_census_night_population_counts_2006_2013_2018_nz/part-00006-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-253-1-c000.snappy.parquet</td><td>part-00006-tid-5626976569612752113-51097d54-ac44-42e0-b210-cf5d6502cbf9-253-1-c000.snappy.parquet</td><td>2995091</td></tr></tbody></table></div>

### Summary

We now have a virtual Hive table we can query using SQL, and we can create a DataFrame using an SQL query, a reference to the Hive table or by reading the underlying Parquet files.

And since the underlying files are Snappy-compressed Parquet, the underlying filesize has gone from 800Mb CSVs (100Mb compressed) to 50Mb Parquet files (even though we denormalised!).
