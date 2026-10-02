"""sparkmeter: see what a Spark query did, in a Glue notebook (or any PySpark session): its stages, tasks, plan and
files read; compare settings and core counts.

In a Glue notebook, before the session starts:
    %%configure
    {"--extra-py-files": "s3://436c-2026w1/l7/student/sparkmeter.py"}
(on EMR: sc.addPyFile("s3://436c-2026w1/l7/student/sparkmeter.py"))

Then:
    import sparkmeter as sm
    sm.setup(spark, save_to="s3://YOUR-BUCKET/sparkmeter/")      # checks the session, counts the cores

    june.collect()                          # your query, in a plain Spark cell
    sm.capture("june", june)                # in the next cell: record what that query did
    sm.checklist("june")                    # how it read its files: folders, scan tasks, row groups, rows
    sm.stage_table("june")                  # L6's table; sm.timeline("june") then %matplot plt; sm.allocation("june")

    def june_by_type(): return spark.read.parquet(...).groupBy(...).agg(...)        # a query as a function
    sm.compare("june by type", june_by_type, {"200": {"spark.sql.shuffle.partitions": "200"},
                                              "1000": {"spark.sql.shuffle.partitions": "1000"}})
    with sm.track("write"): df.write.parquet(...)                 # a cell with several queries, as one run
    sm.tree("s3://.../folders/"); sm.footer("s3://.../part-0.parquet")   # what is stored, before reading it
    sm.report()                                                  # every saved run, one column per core count

How it works: the driver keeps a status store of every query, job, stage and task (the Spark UI is built on it;
Glue notebook sessions run with the UI off, so this reads the store directly through py4j). Everything recorded is
written to save_to as one JSON file per run, so tables and pictures can be redrawn after the session ends.
"""
import datetime, json, statistics, time, urllib.parse

CONFIGS = {  # name -> Spark SQL settings applied for the measurement, then put back
    "default": {},
    "spark-200": {"spark.sql.shuffle.partitions": "200"},
    "emr-1000": {"spark.sql.shuffle.partitions": "200", "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "1000"},
    "adaptive-off": {"spark.sql.adaptive.enabled": "false"},
    "broadcast-off": {"spark.sql.autoBroadcastJoinThreshold": "-1"},
    "split-32mb": {"spark.sql.files.maxPartitionBytes": "32m"},
}
_S = {"warm": set(), "spark": None, "store": None, "jvm": None, "gateway": None, "save_to": None, "cores": None, "executors": None, "session": None, "runs": []}
ROLE_COLOUR = {"scan stage": "#002145", "stage after an Exchange": "#9a4a17", "another input": "#6f9bd1",
               "final totals": "#e0b27a", "schema job (reads footers)": "#7D8998"}   # same colours as the L6 figures


# ---------------------------------------------------------------- setup
def setup(spark, save_to=None):
    """Connect to the driver's status store, count executor cores, remember where to save. Prints what it found."""
    sc = spark.sparkContext
    _S.update(spark=spark, store=sc._jsc.sc().statusStore(), jvm=sc._jvm, gateway=sc._gateway,
              save_to=save_to.rstrip("/") + "/" if save_to else None,
              session=datetime.datetime.now().strftime("%Y%m%d-%H%M%S"), runs=[], warm=set())
    _S["captured_upto"] = _last_execution_id()                    # earlier queries are not this session's to capture
    _count_cores()
    spark.conf.set("spark.sql.maxMetadataStringLength", "2000")    # the plan prints its filters in full
    try:
        import pandas as pd
        pd.set_option("display.width", 250); pd.set_option("display.max_columns", 40)
    except ImportError:
        pass
    now = f"{len(_S['executors'])} executors, {_S['cores']} cores" if _S["cores"] else \
        "executors join with the first query (cores are counted at every measurement)"
    print(f"Spark {spark.version} | {now} | "
          f"shuffle partitions {spark.conf.get('spark.sql.shuffle.partitions')} | "
          f"saving to {_S['save_to'] or 'memory only (report() will see this session only)'}")


def _count_cores():
    """Executors alive right now (not the driver) and their cores."""
    ex = _seq(_S["store"].executorList(True))
    _S["executors"] = {e.id(): e.totalCores() for e in ex if e.id() != "driver"}
    _S["cores"] = sum(_S["executors"].values())


def _seq(x):
    """A Scala Seq from py4j as a Python list."""
    return [x.apply(i) for i in range(x.size())]


# ---------------------------------------------------------------- measuring
def _settings(config):
    """(label, settings) for a config: a name in CONFIGS, names joined by "+", or a dict of Spark settings."""
    if isinstance(config, tuple):                                   # (label, {settings}) from compare
        return config[0], dict(config[1])
    if isinstance(config, dict):
        return "custom", dict(config)
    settings = {}
    for part in config.split("+"):
        settings.update(CONFIGS[part])
    return config, settings


def _run_once(build, action):
    """Call build(); if it returns a DataFrame, run `action` on it (a fresh DataFrame each time)."""
    out = build()
    if hasattr(out, "_jdf"):
        return getattr(out, action)()
    return out


def measure(query, build, config="default", reps=1, note="", first_rep=None, warmup=True, action="collect"):
    """Run a query reps times under `config` and record each run (stages, tasks, plan).
    build: a function returning a DataFrame (built fresh for every run, after the settings are applied), on which
    `action` ("collect", "count") is run; or any function that runs the action itself.
    config: a name in CONFIGS, names joined by "+", or a dict of Spark settings. Returns the last result."""
    if _S["store"] is None:
        raise RuntimeError("Call sm.setup(spark, save_to=...) first.")
    spark, sc = _S["spark"], _S["spark"].sparkContext
    cname, settings = _settings(config)
    before = {k: _conf_or_none(spark, k) for k in settings}
    if first_rep is None:                                         # carry on numbering after earlier runs
        first_rep = 1 + sum(1 for r in _S["runs"] if r["query"] == query and r["config"] == cname)
    try:
        for k, v in settings.items():
            spark.conf.set(k, v)
        if warmup and (query, cname) not in _S["warm"]:            # a query's first run is slower (plan, listing)
            print(f"{query} | {cname}: one warm-up run, not measured")
            sc.setJobGroup(f"sm/{_S['session']}/{query}/warmup", "warmup")
            try:
                _run_once(build, action)
            finally:
                sc.setJobGroup("", "")
            _S["warm"].add((query, cname))
        for rep in range(first_rep, first_rep + reps):
            group = f"sm/{_S['session']}/{query}/{cname}/{rep}/{len(_S['runs'])}"   # unique even if repeated
            mark = _last_execution_id()
            sc.setJobGroup(group, group)
            t0 = time.time()
            try:
                result = _run_once(build, action)
            finally:
                sc.setJobGroup("", "")
            _record(query, cname, settings, time.time() - t0, _collect(group), _plan_since(mark), note=note, rep=rep)
    finally:
        for k, v in before.items():
            spark.conf.unset(k) if v is None else spark.conf.set(k, v)
    return result


def compare(query, build, configs, reps=1, warmup=False, action="collect"):
    """One query under several configs, in rounds: each round runs every config once, starting with a different one
    each round (the first runs of a session are slower, so a fixed order would make the first config look slow).
    configs: a list of names, or a dict {label: {spark setting: value}}. build: a function returning a DataFrame.
    warmup=False assumes you ran the query once yourself already."""
    items = list(configs.items()) if isinstance(configs, dict) else [(c, c) for c in configs]
    for rnd in range(reps):
        k = rnd % len(items)
        for label, config in items[k:] + items[:k]:
            measure(query, build, config=(label, config) if isinstance(configs, dict) else config, reps=1,
                    warmup=warmup, action=action)


def capture(query, df=None, config="default", note=""):
    """Record the query that ran last (its jobs, stages, tasks and final plan) under a name, after you ran it
    yourself in a plain Spark cell. config is only a label for the settings you used ("adaptive off").
    Run it right after the action: if several queries ran since the last capture, the last one is kept."""
    if _S["store"] is None:
        raise RuntimeError("Call sm.setup(spark, save_to=...) first.")
    new = [e for e in _executions() if e["id"] > _S.get("captured_upto", -1)]
    with_jobs = [e for e in new if e["jobs"]]
    if not with_jobs:
        print("No Spark job ran since the last capture: run the query first, then capture it."
              + (" (The last query ran without any Spark job, so there is nothing to record.)" if new else ""))
        _S["captured_upto"] = max([e["id"] for e in new] + [_S.get("captured_upto", -1)])
        return None
    roots = sorted({e["root"] for e in with_jobs})
    newest = max(new, key=lambda e: e["id"])
    if not newest["jobs"] and newest["root"] not in roots:
        print("(the last query ran without any Spark job; recording the last one that did)")
    elif len(roots) > 1:
        print(f"(recording the last of the {len(roots)} queries run since the previous capture)")
    last = [e for e in with_jobs if e["root"] == roots[-1]]
    jobs = {j for e in last for j in e["jobs"]}
    wall = (max(e["end"] or e["start"] for e in last) - min(e["start"] for e in last)) / 1000
    main = roots[-1] if any(e["id"] == roots[-1] for e in new) else max(e["id"] for e in last)   # the main query's plan
    run = _record(query, config, {}, wall, _collect_jobs(jobs), _plan_facts(main), note=note)
    _S["captured_upto"] = max(e["id"] for e in new)
    return run


class track:
    """with sm.track("write salted"): ... runs the cell's Spark actions under a job group and records them all
    (several queries, e.g. a write and its commit, become one run)."""
    def __init__(self, query, config="default"):
        self.query, self.config = query, config

    def __enter__(self):
        sc = _S["spark"].sparkContext
        self.group = f"sm/{_S['session']}/{self.query}/{self.config}/track{len(_S['runs'])}"
        self.mark, self.t0 = _last_execution_id(), time.time()
        sc.setJobGroup(self.group, self.group)
        return self

    def __exit__(self, *exc):
        _S["spark"].sparkContext.setJobGroup("", "")
        if exc[0] is None:
            _record(self.query, self.config, {}, time.time() - self.t0, _collect(self.group), _plan_since(self.mark))
            _S["captured_upto"] = _last_execution_id()
        return False


def _record(query, cname, settings, wall, stages, plan, note="", rep=None):
    spark = _S["spark"]
    _count_cores()
    if rep is None:
        rep = 1 + sum(1 for r in _S["runs"] if r["query"] == query and r["config"] == cname)
    run = dict(query=query, config=cname, settings=settings, rep=rep, wall_s=round(wall, 3), cores=_S["cores"],
               executors=_S["executors"], session=_S["session"], note=note, spark=spark.version, stages=stages,
               plan=plan)
    _S["runs"].append(run)
    _save(run)
    print(f"{query} | {cname} | run {rep}: {wall:.2f} s, {len(stages)} stages")
    return run


# ---------------------------------------------------------------- the SQL status store: queries and their plans
def _sql_store():
    return _S["spark"]._jsparkSession.sharedState().statusStore()


def _executions():
    """Every SQL query the session has run: id, root id, start/end (ms), job ids."""
    out = []
    for e in _seq(_sql_store().executionsList()):
        end = e.completionTime()
        out.append(dict(id=int(e.executionId()), root=int(e.rootExecutionId()), start=int(e.submissionTime()),
                        end=int(end.get().getTime()) if end.isDefined() else None,
                        jobs=[int(k) for k in _seq(e.jobs().keys().toSeq())]))
    return out


def _last_execution_id():
    ids = [e["id"] for e in _executions()]
    return max(ids) if ids else -1


def _plan_since(mark):
    """Plan facts of the last query with jobs that started after execution id `mark`."""
    ids = [e["id"] for e in _executions() if e["id"] > mark and e["jobs"]]
    return _plan_facts(max(ids)) if ids else {}


def _plan_facts(eid):
    """From one query's final plan: PartitionFilters, PushedFilters, footer answers, and the scan's metrics
    (files read, folders read), summed over the plan's file scans."""
    import re
    st = _sql_store()
    e = st.execution(eid).get()
    final = e.physicalPlanDescription()        # "formatted": node details follow the plan trees, so keep it whole
    grab = lambda k: list(dict.fromkeys(re.sub(r"#\d+", "", m.strip())
                                        for m in re.findall(rf"{k}: \[(.*?)\](?:,|\s|$)", final) if m.strip()))
    values = st.executionMetrics(eid)
    metrics = {"number of files read": 0, "number of partitions read": 0}
    found, scans = set(), 0
    for node in _seq(st.planGraph(eid).allNodes()):
        if not node.name().startswith("Scan"):
            continue
        scans += 1
        for m in _seq(node.metrics()):
            if m.name() in metrics:
                v = values.get(m.accumulatorId())
                if v.isDefined():
                    metrics[m.name()] += int(str(v.get()).replace(",", "").split()[0])
                    found.add(m.name())
    return dict(partition_filters=grab("PartitionFilters"), pushed_filters=grab("PushedFilters"),
                footer_answer=bool(grab("PushedAggregation")) or "EnablePushdownAggregate: true" in final,
                files_read=metrics["number of files read"] if "number of files read" in found else None,
                folders_read=metrics["number of partitions read"] if "number of partitions read" in found else None,
                scans=scans, plan=final[:20000])


def _conf_or_none(spark, key):
    try:
        return spark.conf.get(key)
    except Exception:
        return None


def _settle(match, timeout_s=5.0):
    """The status store is filled by a listener a moment after the action returns: wait until the jobs are done."""
    st, t0 = _S["store"], time.time()
    while time.time() - t0 < timeout_s:
        jobs = [j for j in _seq(st.jobsList(None)) if match(j)]
        if jobs and all(str(j.status()) != "RUNNING" for j in jobs):
            return
        time.sleep(0.2)


def _collect(group):
    st = _S["store"]
    _settle(lambda j: j.jobGroup().isDefined() and j.jobGroup().get() == group)
    return _collect_stages(sorted({int(x) for j in _seq(st.jobsList(None))
                                   if j.jobGroup().isDefined() and j.jobGroup().get() == group
                                   for x in _seq(j.stageIds())}))


def _collect_jobs(job_ids):
    st = _S["store"]
    _settle(lambda j: j.jobId() in job_ids)
    return _collect_stages(sorted({int(x) for j in _seq(st.jobsList(None)) if j.jobId() in job_ids
                                   for x in _seq(j.stageIds())}))


def _collect_stages(ids):
    st, jvm = _S["store"], _S["jvm"]
    no_status, no_quantiles = jvm.java.util.ArrayList(), _S["gateway"].new_array(jvm.double, 0)
    out = []
    for sid in ids:
        for att in _seq(st.stageData(sid, False, no_status, False, no_quantiles)):
            if str(att.status()) != "COMPLETE":
                continue                                          # skipped: output reused, never ran
            tasks = []
            for t in _seq(st.taskList(sid, att.attemptId(), 100000)):
                if t.status() != "SUCCESS" or not t.taskMetrics().isDefined():
                    continue
                m = t.taskMetrics().get()
                tasks.append(dict(executor=t.executorId(), launch_ms=t.launchTime().getTime(),
                                  dur_s=t.duration().get() / 1000, rows_read=m.inputMetrics().recordsRead(),
                                  rows_from_exchange=m.shuffleReadMetrics().recordsRead(),
                                  rows_into_exchange=m.shuffleWriteMetrics().recordsWritten(),
                                  bytes_into_exchange=m.shuffleWriteMetrics().bytesWritten(),
                                  spill_disk=m.diskBytesSpilled()))
            out.append(dict(stage=sid, name=att.name(), tasks=tasks))
    return out


def _save(run):
    if not _S["save_to"]:
        return
    import boto3
    u = urllib.parse.urlsplit(_S["save_to"])
    key = f"{u.path.lstrip('/')}{run['session']}-{run['cores']}c-{run['query']}-{run['config']}-{run['rep']}.json"
    boto3.client("s3").put_object(Bucket=u.netloc, Key=key, Body=json.dumps(run).encode())


def _runs(query=None, config=None, everything=False):
    runs = _load_all() if everything else _S["runs"]
    return [r for r in runs if (query is None or r["query"] == query) and (config is None or r["config"] == config)]


def _load_all():
    if not _S["save_to"]:
        return list(_S["runs"])
    if not _S["save_to"].startswith("s3://"):                      # a local folder of saved runs
        import glob, os
        return [json.load(open(f)) for f in sorted(glob.glob(os.path.join(_S["save_to"], "*.json")))]
    import boto3
    s3 = boto3.client("s3"); u = urllib.parse.urlsplit(_S["save_to"]); out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=u.netloc, Prefix=u.path.lstrip("/")):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".json"):
                out.append(json.loads(s3.get_object(Bucket=u.netloc, Key=o["Key"])["Body"].read()))
    return out


def open_saved(save_to):
    """Load every run saved under save_to, without Spark (e.g. on your laptop, or after the session ended).
    Then stage_table, allocation, timeline and report work on them; timeline(..., cores=16) picks a core count."""
    _S["save_to"] = save_to.rstrip("/") + "/"
    _S["runs"] = _load_all()
    print(f"{len(_S['runs'])} runs: " + ", ".join(sorted({f"{r['query']}/{r['config']}@{r['cores']}c" for r in _S['runs']})[:12]) + " ...")


# ---------------------------------------------------------------- reading files: the six steps
def checklist(query, config=None, rep=None):
    """How a recorded query read its files, in the order Spark does it: folders kept (PartitionFilters), files and
    scan tasks, the conditions sent to the footers (PushedFilters), reading tasks, rows read."""
    run = _pick(query, config, rep)
    p = run.get("plan") or {}
    scan = max(run["stages"], key=lambda st: sum(t["rows_read"] for t in st["tasks"]), default=None)
    tasks = scan["tasks"] if scan else []
    reading = sum(1 for t in tasks if t["rows_read"])
    rows = sum(t["rows_read"] for t in tasks)
    short = lambda xs: "; ".join(x if len(x) <= 160 else x[:159] + "…" for x in xs) or "(none)"
    folders = f"  ->  {p['folders_read']} folders kept" if p.get("folders_read") is not None else ""
    print(f"{query} | {run['config']} | {run['wall_s']:.2f} s"
          + (f"  (this query reads {p['scans']} inputs: files and folders below add them up)" if p.get("scans", 1) > 1 else ""))
    print(f"1-2. folders   PartitionFilters: {short(p.get('partition_filters', []))}{folders}")
    print(f"3.   ranges    {p.get('files_read', '?')} files  ->  {len(tasks)} scan tasks")
    print(f"4-5. footers   PushedFilters: {short(p.get('pushed_filters', []))}")
    print(f"               {reading} of {len(tasks)} scan tasks still hold a row group (reading tasks)")
    marker = "EnablePushdownAggregate: true" if "EnablePushdownAggregate: true" in p.get("plan", "") else "PushedAggregation"
    print(f"6.   read      {rows:,} rows" + (f"  (answered from the footers, one row per row group: the plan's scan says {marker})"
                                              if p.get("footer_answer") else ""))


def footer(path, column="all", between=None, region="ca-central-1"):
    """What one Parquet file's footer says about its row groups, without reading the data (a few KB).
    column="all": one row per row group and column, with the column's type, min, max, nulls and MiB.
    column="<name>": one row per row group for that column; between=(low, high) then adds whether a condition
    low <= column < high could match the row group (the test a scan task makes before reading it)."""
    import pandas as pd, pyarrow.parquet as pq
    fs, p = _fs(path, region)
    md = pq.ParquetFile(p, filesystem=fs).metadata
    names = [md.schema.column(k).name for k in range(md.num_columns)]
    types = {f.name: str(f.type) for f in md.schema.to_arrow_schema()}
    cols = names if column == "all" else [column]
    if between is not None and column == "all":
        raise ValueError("between= needs one column: footer(path, column='tpep_pickup_datetime', between=(...))")
    rows = []
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        for c in cols:
            ch = rg.column(names.index(c)); st = ch.statistics
            r = {"row group": g + 1, "column": c, "type": types.get(c, "?"), "rows": rg.num_rows,
                 "min": st.min if st is not None and st.has_min_max else None,
                 "max": st.max if st is not None and st.has_min_max else None,
                 "nulls": st.null_count if st is not None else None,
                 "MiB": round(ch.total_compressed_size / 2**20, 2)}
            if between is not None:
                is_time = types.get(c, "").startswith("timestamp")
                lo, hi, mn, mx = (_bound(v, is_time) for v in (*between, r["min"], r["max"]))
                r["could match"] = None if mn is None or mx is None else bool(mn < hi and mx >= lo)   # None: no statistics
            rows.append(r)
    print(f"{path.rsplit('/', 2)[-2]}/{path.rsplit('/', 1)[-1]}: {md.num_row_groups} row group{'s' if md.num_row_groups != 1 else ''}, "
          f"{md.num_rows:,} rows, {md.num_columns} columns")
    df = pd.DataFrame(rows)
    if column == "all":
        return df.set_index(["row group", "column"])
    return df.drop(columns=["column", "type"]).set_index("row group")


def tree(path, row_groups=False, show_first=2, show_last=1, region="ca-central-1"):
    """The folders and files under an S3 path, drawn as a tree: the first two folders, the last one, and a line
    saying what is left out in between. Sizes are rounded. row_groups=True adds, under each file, how many row
    groups its footer lists (reads each shown file's footer, a few KB)."""
    import boto3, pyarrow.parquet as pq
    bucket, prefix = path[len("s3://"):].split("/", 1)
    prefix = prefix.rstrip("/") + "/"
    s3 = boto3.client("s3", region_name=region)
    folders = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            d, _, f = rel.rpartition("/")
            if f and not f.startswith(("_", ".")):                     # skip _SUCCESS and hidden files
                folders.setdefault(d, []).append((f, o["Size"], o["Key"]))
    names = sorted(folders)
    if len(names) > show_first + show_last + 1:
        keep = names[:show_first] + [None] + names[-show_last:]
    else:
        keep = names
    key = names[0].split("/")[-1].split("=")[0] if names and "=" in names[0] else None
    fs = _fs(path, region)[0] if row_groups else None
    size = lambda b: f"about {b / 1e9:.1f} GB" if b >= 1e9 else f"about {b / 1e6:.0f} MB" if b >= 1e6 else f"about {b / 1e3:.0f} KB"
    width = max([len(f) for fl in folders.values() for f, _, _ in fl] + [10]) + 2
    lines = [prefix.rstrip("/").split("/")[-1] + "/"]
    for i, d in enumerate(keep):
        last = i == len(keep) - 1
        if d is None:
            what = f"one folder for each {key} value" if key else "more folders like these"
            lines.append(f"│   {'…':<{width + 4}}{what}")
            continue
        pad = "    " if last else "│   "
        files = folders[d]
        if d:
            lines.append(("└── " if last else "├── ") + d + "/")
        else:
            pad = ""
        shown = files if len(files) <= show_first + show_last + 1 else files[:show_first] + [None] + files[-show_last:]
        for j, entry in enumerate(shown):
            if entry is None:
                lines.append(f"{pad}│   {'…':<{width}}more files like these")
                continue
            f, b, k = entry
            flast = j == len(shown) - 1
            lines.append(f"{pad}{'└── ' if flast else '├── '}{f:<{width}}{size(b)}")
            if row_groups and f.endswith(".parquet"):
                md = pq.ParquetFile(f"{bucket}/{k}", filesystem=fs).metadata
                n = md.num_row_groups
                lines.append(f"{pad}{'    ' if flast else '│   '}└── {n} row group{'s' if n != 1 else ''}, "
                             f"about {md.num_rows // max(n, 1):,} rows each")
    print("\n".join(lines))


def _fs(path, region):
    import pyarrow.fs as pf
    if path.startswith("s3://"):
        return pf.S3FileSystem(region=region), path[len("s3://"):]
    return pf.LocalFileSystem(), path


def _bound(v, is_time):
    """A footer value or a condition bound, comparable with the others. For time columns: strings are parsed as times,
    and times with a zone are converted to UTC and the zone dropped (a UTC-adjusted column's statistics come back with
    one; Glue sessions run in UTC, so a bound without a zone is read as UTC). Other columns: unchanged."""
    import datetime as dt, pandas as pd
    if v is None or not is_time:
        return v
    if isinstance(v, (str, dt.datetime, pd.Timestamp)):
        t = pd.Timestamp(v)
        return t.tz_convert("UTC").tz_localize(None) if t.tzinfo else t
    return v


# ---------------------------------------------------------------- reading one run
def _role(stage, scan_rows, last=None):
    ts = stage["tasks"]
    rows = sum(t["rows_read"] for t in ts); from_ex = sum(t["rows_from_exchange"] for t in ts)
    if from_ex:
        return "final totals" if len(ts) == 1 and stage["stage"] == last else "stage after an Exchange"
    if rows == 0:
        return "schema job (reads footers)" if "parquet" in stage.get("name", "").lower() else "no input rows"
    return "scan stage" if rows == scan_rows else "another input"  # the stage reading the most rows is the scan


def _pick(query, config=None, rep=None, cores=None):
    rs = [r for r in _runs(query, config) if cores is None or r["cores"] == cores]
    if not rs:
        raise ValueError(f"No run of {query!r} with config {config!r} in this session. Measured: "
                         f"{sorted({(r['query'], r['config']) for r in _S['runs']})}")
    return rs[-1] if rep is None else next(r for r in rs if r["rep"] == rep)


def stage_table(query, config=None, rep=None, cores=None):
    """L6's table for one run: per stage, tasks, tasks with rows, rows, what went into an Exchange, task times."""
    import pandas as pd
    run = _pick(query, config, rep, cores)
    scan_rows = max((sum(t["rows_read"] for t in s["tasks"]) for s in run["stages"]), default=0)
    rows = []
    for s in run["stages"]:
        ts = s["tasks"]; d = [t["dur_s"] for t in ts]
        rows.append({"stage": s["stage"], "does": _role(s, scan_rows, run['stages'][-1]['stage']), "tasks": len(ts),
                     "tasks with rows": sum(1 for t in ts if t["rows_read"] or t["rows_from_exchange"]),
                     "rows read": sum(t["rows_read"] for t in ts),
                     "rows from Exchange": sum(t["rows_from_exchange"] for t in ts),
                     "rows to Exchange": sum(t["rows_into_exchange"] for t in ts),
                     "MB to Exchange": round(sum(t["bytes_into_exchange"] for t in ts) / 1e6, 2),
                     "spill to disk MB": round(sum(t["spill_disk"] for t in ts) / 1e6, 1),
                     "longest task s": round(max(d), 2), "median task s": round(statistics.median(d), 2)})
    print(f"{query} | {run['config']} | run {run['rep']} | {run['cores']} cores | {run['wall_s']:.2f} s")
    return pd.DataFrame(rows).set_index("stage")


def allocation(query, config=None, rep=None, cores=None):
    """Tasks, rows and busy seconds per executor, per stage: how the work was allocated."""
    import pandas as pd
    run = _pick(query, config, rep, cores)
    scan_rows = max((sum(t["rows_read"] for t in s["tasks"]) for s in run["stages"]), default=0)
    rows = []
    for s in run["stages"]:
        role = _role(s, scan_rows, run['stages'][-1]['stage'])
        for ex in sorted({t["executor"] for t in s["tasks"]}):
            ts = [t for t in s["tasks"] if t["executor"] == ex]
            rows.append({"stage": s["stage"], "does": role, "executor": ex, "tasks": len(ts),
                         "rows": sum(t["rows_read"] + t["rows_from_exchange"] for t in ts),
                         "busy s": round(sum(t["dur_s"] for t in ts), 2)})
    return pd.DataFrame(rows).set_index(["stage", "executor"])


def timeline(query, config=None, rep=None, xmax=None, show_footers=False, title=None, cores=None,
             other_label="reads another input", final_label="final totals"):
    """Which core ran which task, when. One row per core, grouped by executor; colour = the stage, hatched = the task
    read 0 rows (same words and colours as the L6 figures). Name the second input and the last stage for your query,
    e.g. other_label="reads the name table", final_label="totals per name". In a Glue notebook: %matplot plt."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    run = _pick(query, config, rep, cores)
    scan_rows = max((sum(t["rows_read"] for t in s["tasks"]) for s in run["stages"]), default=0)
    ts_of = lambda ms: ms / 1000
    bars = [(t, _role(s, scan_rows, run['stages'][-1]['stage'])) for s in run["stages"] for t in s["tasks"]]
    bars = [(t, r) for t, r in bars if show_footers or r != "schema job (reads footers)"]
    t0 = min(ts_of(t["launch_ms"]) for t, _ in bars)
    lanes, placed = {}, []
    for t, role in sorted(bars, key=lambda b: ts_of(b[0]["launch_ms"])):
        a = ts_of(t["launch_ms"]) - t0; b = a + t["dur_s"]
        free = lanes.setdefault(t["executor"], [])
        k = next((i for i, end in enumerate(free) if end <= a + 0.005), None)
        if k is None:
            free.append(b); k = len(free) - 1
        else:
            free[k] = b
        placed.append((t["executor"], k, a, t["dur_s"], role, not (t["rows_read"] or t["rows_from_exchange"])))
    exs = sorted(lanes, key=lambda e: (len(e), e))
    base, y = {}, 0
    for e in exs:
        base[e] = y; y += max(len(lanes[e]), run["executors"].get(e, 0))
    fig, ax = plt.subplots(figsize=(10, 0.42 * y + 2.2))
    fig.patch.set_facecolor("#fbf9f4"); ax.set_facecolor("#fbf9f4")
    for e, k, a, w, role, empty in placed:
        c = ROLE_COLOUR[role]
        ax.barh(base[e] + k, max(w, 0.004), left=a, height=0.8,
                **({"facecolor": "none", "edgecolor": c, "hatch": "///"} if empty else {"color": c, "edgecolor": "white"}))
    for e in exs[1:]:
        ax.axhline(base[e] - 0.5, color="#7D8998", lw=0.8)
    ax.set_yticks([base[e] + max(len(lanes[e]), run["executors"].get(e, 0)) / 2 - 0.5 for e in exs])
    ax.set_yticklabels([f"executor {e}\n({run['executors'].get(e, '?')} cores)" for e in exs])
    ax.set_ylim(y - 0.5, -0.5); ax.set_xlabel("seconds since first task")
    if xmax: ax.set_xlim(0, xmax)
    for side in ("top", "right"): ax.spines[side].set_visible(False)
    names = {"another input": other_label, "final totals": final_label}
    def label(r, empty):
        if r in ("scan stage", "stage after an Exchange"):
            verb = "got" if r == "stage after an Exchange" else "read"
            return f"{r}: task {verb} {'0 rows' if empty else 'rows'}"
        return names.get(r, r) + (": task read 0 rows" if empty else "")
    handles = []
    for r in ROLE_COLOUR:
        mine = [p for p in placed if p[4] == r]
        if any(not p[5] for p in mine) or (mine and r == "schema job (reads footers)"):
            handles.append(Patch(color=ROLE_COLOUR[r], label=label(r, False)))
        if any(p[5] for p in mine) and r != "schema job (reads footers)":
            handles.append(Patch(facecolor="none", edgecolor=ROLE_COLOUR[r], hatch="///", label=label(r, True)))
    ax.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=2, frameon=False)
    ax.set_title(title or f"{query} | {run['config']} | {run['cores']} cores | rep {run['rep']} | {run['wall_s']:.2f} s",
                 loc="left", fontsize=11)
    fig.tight_layout()
    return fig


# ---------------------------------------------------------------- comparing
def report(everything=True):
    """One row per query and config, one column per core count: median query time over reps, and from the busiest
    stage after an Exchange, the tasks with rows and the largest task's rows. everything=True reads every saved
    session in save_to; False uses this session only."""
    import pandas as pd
    rows = []
    for r in _runs(everything=everything):
        after = [s for s in r["stages"] if sum(t["rows_from_exchange"] for t in s["tasks"])]
        busiest = max(after, key=lambda s: sum(t["rows_from_exchange"] for t in s["tasks"]), default=None)
        scan = max(r["stages"], key=lambda s: sum(t["rows_read"] for t in s["tasks"]), default=None)
        rows.append(dict(query=r["query"], config=r["config"], cores=r["cores"], wall_s=r["wall_s"],
                         scan_tasks=len(scan["tasks"]) if scan else 0,
                         scan_reading=sum(1 for t in scan["tasks"] if t["rows_read"]) if scan else 0,
                         after_tasks=len(busiest["tasks"]) if busiest else 0,
                         after_with_rows=sum(1 for t in busiest["tasks"] if t["rows_from_exchange"]) if busiest else 0,
                         largest_task_rows=max((t["rows_from_exchange"] for t in busiest["tasks"]), default=0) if busiest else 0))
    if not rows:
        print("Nothing measured yet."); return None
    df = pd.DataFrame(rows)
    g = df.groupby(["query", "config", "cores"])
    t = g.agg(reps=("wall_s", "size"), median_s=("wall_s", "median"), scan_tasks=("scan_tasks", "first"),
              scan_reading=("scan_reading", "first"), after_tasks=("after_tasks", "first"),
              after_with_rows=("after_with_rows", "first"), largest_task_rows=("largest_task_rows", "first"))
    t["scan"] = t.scan_reading.astype(str) + " of " + t.scan_tasks.astype(str)
    t["after Exchange"] = t.after_with_rows.astype(str) + " of " + t.after_tasks.astype(str)
    t["largest task (M rows)"] = (t.largest_task_rows / 1e6).round(2)
    wide = t[["median_s", "scan", "after Exchange", "largest task (M rows)"]].unstack("cores")
    wide.columns = [f"{m} @ {c} cores" for m, c in wide.columns]
    return wide
