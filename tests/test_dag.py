"""DAG structure tests.

With Airflow installed (CI / Composer image):  pytest tests/test_dag.py   -> uses the real DagBag.
Without Airflow (e.g. a locked-down sandbox): lightweight stand-ins for the Airflow classes are used, which still
checks that the file imports, task ids are unique, the graph is acyclic and the key ordering rules hold.
The stand-in does NOT prove Airflow accepts the DAG - run the real DagBag test (or `airflow dags list-import-errors`) too.
"""
import importlib.util
import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / "dags" / "grocery_daily_batch.py"
os.environ.setdefault("GROCERY_PLATFORM_DIR", str(ROOT))  # sql/, pipeline/, campaign/ live at the repo root locally

try:
    import airflow  # noqa: F401
    REAL = True
except ImportError:
    REAL = False


# ---------------------------------------------------------------- minimal stand-ins
def install_stubs():
    class Node:
        def __init__(self):
            self.up, self.down = set(), set()

        def _targets(self, other):
            return list(other) if isinstance(other, (list, tuple)) else [other]

        def __rshift__(self, other):
            for t in self._targets(other):
                for s in self.leaves():
                    for d in t.roots():
                        s.down.add(d)
                        d.up.add(s)
            return other

        def __rrshift__(self, other):
            for s in self._targets(other):
                s >> self
            return self

    class Operator(Node):
        registry = []
        group_stack = []

        def __init__(self, task_id, **kw):
            super().__init__()
            prefix = "".join(g.group_id + "." for g in Operator.group_stack)
            self.task_id = prefix + task_id
            self.kw = kw
            assert self.task_id not in [o.task_id for o in Operator.registry], f"duplicate task id {self.task_id}"
            Operator.registry.append(self)
            if Operator.group_stack:
                Operator.group_stack[-1].children.append(self)

        def leaves(self): return [self]
        def roots(self): return [self]

    class TaskGroup(Node):
        def __init__(self, group_id):
            super().__init__()
            self.group_id, self.children = group_id, []

        def __enter__(self): Operator.group_stack.append(self); return self
        def __exit__(self, *a): Operator.group_stack.pop()
        def leaves(self): return [c for c in self.children if not (c.down & set(self.children))]
        def roots(self): return [c for c in self.children if not (c.up & set(self.children))]

    class DAG:
        def __init__(self, dag_id, **kw): self.dag_id, self.kw = dag_id, kw
        def __enter__(self): return self
        def __exit__(self, *a): pass

    def mod(name, **attrs):
        m = types.ModuleType(name)
        m.__dict__.update(attrs)
        sys.modules[name] = m
        return m

    class Var:
        values = {}
        @staticmethod
        def get(k, default_var=None): return Var.values.get(k, default_var)

    class Fail(Exception): pass

    import datetime as _dt
    mod("pendulum", timezone=lambda n: _dt.timezone(_dt.timedelta(hours=5, minutes=30)),
        datetime=lambda *a, tz=None: _dt.datetime(*a, tzinfo=tz), now=lambda tz=None: _dt.datetime.now(tz))
    mod("airflow", DAG=DAG)
    mod("airflow.exceptions", AirflowFailException=Fail)
    mod("airflow.models", Variable=Var)
    mod("airflow.operators")
    mod("airflow.operators.empty", EmptyOperator=type("EmptyOperator", (Operator,), {}))
    mod("airflow.operators.python", PythonOperator=type("PythonOperator", (Operator,), {}),
        BranchPythonOperator=type("BranchPythonOperator", (Operator,), {}))
    for n in ["airflow.providers", "airflow.providers.google", "airflow.providers.google.cloud",
              "airflow.providers.google.cloud.hooks", "airflow.providers.google.cloud.operators",
              "airflow.providers.google.cloud.sensors", "airflow.utils"]:
        mod(n)
    mod("airflow.providers.google.cloud.hooks.bigquery", BigQueryHook=object)
    mod("airflow.providers.google.cloud.hooks.gcs", GCSHook=object)
    mod("airflow.providers.google.cloud.operators.bigquery",
        BigQueryCreateEmptyDatasetOperator=type("BQDataset", (Operator,), {}),
        BigQueryInsertJobOperator=type("BQJob", (Operator,), {}))
    mod("airflow.providers.google.cloud.operators.dataflow",
        DataflowCreatePythonJobOperator=type("Dataflow", (Operator,), {}))
    mod("airflow.providers.google.cloud.sensors.gcs", GCSObjectExistenceSensor=type("GCSSensor", (Operator,), {}))
    mod("airflow.utils.task_group", TaskGroup=TaskGroup)
    mod("airflow.utils.trigger_rule", TriggerRule=types.SimpleNamespace(
        NONE_FAILED="none_failed", NONE_FAILED_MIN_ONE_SUCCESS="none_failed_min_one_success"))
    return Operator, Var, Fail


def load_dag_module():
    spec = importlib.util.spec_from_file_location("grocery_daily_batch", DAG_FILE)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# ---------------------------------------------------------------- tests
def build_graph():
    if REAL:
        from airflow.models import DagBag
        bag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
        assert not bag.import_errors, bag.import_errors
        dag = bag.dags["grocery_daily_batch"]
        return {t.task_id: {d for d in t.downstream_task_ids} for t in dag.tasks}
    Operator, _, _ = install_stubs()
    load_dag_module()
    return {o.task_id: {d.task_id for d in o.down} for o in Operator.registry}


def reachable(graph, a, b):
    seen, stack = set(), [a]
    while stack:
        n = stack.pop()
        for d in graph[n]:
            if d == b:
                return True
            if d not in seen:
                seen.add(d)
                stack.append(d)
    return False


def test_graph():
    g = build_graph()
    # acyclic
    for t in g:
        assert not reachable(g, t, t), f"cycle through {t}"
    feeds = ["transactions", "customers", "products", "availability", "expenses", "festivals"]
    for f in feeds:
        assert f"wait_for_files.wait_{f}" in g
        assert reachable(g, f"wait_for_files.wait_{f}", "load_facts") and reachable(g, f"wait_for_files.wait_{f}", "load_dimensions")
    # ordering rules that protect production data
    order = [("create_dead_letter_table", "load_facts"), ("validate_headers", "load_dimensions"),
             ("load_facts", "build_models"), ("load_dimensions", "build_models"),
             ("build_models", "forecast_and_plan"), ("train_model", "forecast_and_plan"),
             ("skip_training", "forecast_and_plan"), ("forecast_and_plan", "customer_models"),
             ("customer_models", "email_campaign"), ("email_campaign", "run_summary"), ("run_summary", "end")]
    for a, b in order:
        assert reachable(g, a, b), f"{a} must run before {b}"
    dq = [t for t in g if t.startswith("data_quality.dq_")]
    assert len(dq) == 6, dq
    for t in dq:  # every quality gate sits between the loads and the transformations
        assert reachable(g, "load_facts", t) and reachable(g, t, "build_models"), t
        assert reachable(g, t, "email_campaign"), t
    assert not reachable(g, "train_model", "skip_training") and not reachable(g, "skip_training", "train_model")
    print(f"OK: {len(g)} tasks, {sum(len(v) for v in g.values())} edges")


if __name__ == "__main__":
    test_graph()
