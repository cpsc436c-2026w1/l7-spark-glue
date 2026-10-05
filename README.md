# L7: Spark on AWS Glue

Before you start:

> Billing stops only when the session stops. Closing the notebook tab or the browser **does not stop it**: you will continue to pay for 3 workers (in the default demo setup) approx. $0.02 **a minute**.

## 1. Set up your account (once)

This creates a bucket for your results + a role that Glue sessions run as.

1. Sign in to your AWS sandbox and check that the region at the top right is **Canada (Central)**.
2. Open **CloudShell**: the `>_` icon in the top bar. A terminal opens at the bottom of the page.
3. Paste this line and press Enter:

   ```
   aws s3 cp s3://436c-2026w1/l7/student/setup_glue.sh . && bash setup_glue.sh
   ```

You should see:

```
bucket  s3://436c-glue-<your account number> created
role    436c-glue-notebook created
role    policies attached
check   course data readable
```

If the script stops with `STOP: this account can't download ...`, send your account number (printed on the
first line) to the teaching team. 

Rerunning the script is safe.

[`setup_glue.sh`](setup_glue.sh) is in this repository if you want to read what it does first.

## 2. Create the notebook

1. Download [`l7a_data-read.ipynb`](l7a_data-read.ipynb) (part B: [`l7b_slow-queries.ipynb`](l7b_slow-queries.ipynb)) to your laptop: open it here and use the download button (**Download raw file**). The Glue console uploads it from your laptop in the next step.
2. In the AWS console, open **AWS Glue**, then **ETL jobs** > **Notebooks** in the menu on the left.
3. Create a notebook, choose to upload your own notebook file, and pick `l7a_data-read.ipynb`.
4. For the IAM role, choose **436c-glue-notebook**. Create the notebook.

The notebook editor opens in a minute or so. If it stays blank, your browser is blocking third-party
cookies; allow them for the AWS console and reload.

## 3. Run it

Run the cells in order. 

The first code cells set the session's size (3 workers: one with Spark driver and two with Spark executors with 4 slots each (4 vCPUs per worker)). It also loads `sparkmeter`, a vibecoded helper for measuring Spark queries. 

The session starts (and you start paying money!) when the first Spark cell runs, which takes about 30 to 60 seconds.


## 4. Stop the session!

> **Billing stops only when the session stops.** Closing the notebook tab or the browser does not stop it:
> the session keeps its 3 workers, and keeps billing, until you stop it or it has been idle for 15 minutes.

Run the last cell, `%stop_session`, when you are done. Three workers cost about $0.02 a minute, so the whole
notebook run should cost about $0.20, and a session forgotten for 15 minutes adds about $0.33.

If you closed the tab without stopping, stop the session from CloudShell:

```
aws glue list-sessions --query "Sessions[?Status=='READY'].Id"
aws glue stop-session --id <an id from that list>
```

**To start again** after a stop (or after the idle timeout), run the first code cells again, from the top
through the Step 0 cell itself: the settings cells, then the Step 0 cell, which starts a new session and loads `sparkmeter`.
The new session has none of the old one's variables, so also rerun the cells that define what your step
uses.

## Files

| File | What it is |
|---|---|
| [`l7a_data-read.ipynb`](l7a_data-read.ipynb) | Part A of the lecture demos: how Spark reads a folder of Parquet files, then exercises to try on your own |
| [`l7b_slow-queries.ipynb`](l7b_slow-queries.ipynb) | Part B (the tutorial): three slow queries from L6 and their fixes, then exercises B1 to B5 with their answers in commented-out cells |
| [`sparkmeter.py`](sparkmeter.py) | The measuring helper. The notebook loads its copy from `s3://436c-2026w1/l7/student/`. |
| [`setup_glue.sh`](setup_glue.sh) | The setup script from step 1 |

## `sparkmeter` functions

The notebook runs each query as plain Spark, then asks `sparkmeter` (`sm`) what that query did.

| Call | Gives |
|---|---|
| `sm.setup(spark, save_to=None)` | Connects to the session; with `save_to`, every recorded run is saved there |
| `sm.capture(name)` or `sm.capture(name, df)` | Records the query that ran last (its jobs, stages, tasks and plan) under `name`. Run it right after the query. |
| `sm.checklist(name)` | How that query read its files: folders kept, scan tasks, reading tasks, rows read |
| `sm.stage_table(name)` | Per stage: tasks, tasks with rows, rows in and out of an `Exchange`, largest task's rows, median rows of the tasks with rows, task times |
| `sm.allocation(name)` | Tasks, rows and busy seconds per executor |
| `sm.compare(name, query_function, settings)` | Runs a query (a function that returns a DataFrame) once under each set of Spark settings, building it fresh each time, and records every run |
| `sm.tree(path)` | The folders and files under an S3 path; `row_groups=True` adds each file's row groups |
| `sm.footer(path)` | What one Parquet file's footer holds: per row group and column, the min, max, nulls and size; `column=` and `between=` ask which row groups could match a condition |
| `sm.report()` | Every saved run, side by side, one column per slot count |

`settings` in `sm.compare` is a dictionary from a label to Spark settings, for example
`{"broadcast on": {"spark.sql.autoBroadcastJoinThreshold": "10m"}, "broadcast off": {"spark.sql.autoBroadcastJoinThreshold": "-1"}}`.
When a run used settings you changed yourself, give `sm.capture` a label for them: `sm.capture("june", config="adaptive off")`.
