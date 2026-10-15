"""Online detectors for the Data Siege event stream.

Each handler makes one metered call to the tool associated with its event
type, then compares the returned measurements with the published baseline.
"""
from api import Verdict
import math


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _has(data, key):
    return isinstance(data, dict) and key in data and data[key] is not None


def _alert(pillar, reason):
    return Verdict(alert=True, pillar=pillar, reason=reason)


def check_data_batch(payload, ctx):
    pillar = "checks"
    profile = ctx.tools.batch_profile(payload.get("batch_id"))
    if not isinstance(profile, dict) or "error" in profile:
        return Verdict(alert=False, pillar=pillar, reason="batch profile unavailable")

    b = ctx.baseline
    signals = []
    if _has(profile, "row_count"):
        rows = profile["row_count"]
        if rows < b.get("row_count_min", -math.inf) or rows > b.get("row_count_max", math.inf):
            signals.append("row_count outside baseline")
    nulls = profile.get("null_rate", {})
    if isinstance(nulls, dict) and _has(nulls, "customer_id"):
        if nulls["customer_id"] > b.get("null_rate_max", math.inf):
            signals.append("customer_id null rate above baseline")
    if _has(profile, "mean_amount"):
        mean = profile["mean_amount"]
        if mean < b.get("mean_amount_min", -math.inf) or mean > b.get("mean_amount_max", math.inf):
            signals.append("mean amount outside baseline")
    if _has(profile, "staleness_min") and profile["staleness_min"] > b.get("staleness_min_max", math.inf):
        signals.append("batch staleness above baseline")

    # Combine several moderate deviations, while avoiding alerts from a single
    # near-boundary measurement. Bounds are treated as calibrated 3-sigma
    # intervals; null and staleness use their published upper bounds.
    distances = []
    if _has(profile, "row_count"):
        lo, hi = b.get("row_count_min"), b.get("row_count_max")
        if lo is not None and hi is not None and hi > lo:
            center, radius = (lo + hi) / 2.0, (hi - lo) / 2.0
            distances.append((profile["row_count"] - center) / radius)
    if _has(profile, "mean_amount"):
        lo, hi = b.get("mean_amount_min"), b.get("mean_amount_max")
        if lo is not None and hi is not None and hi > lo:
            center, radius = (lo + hi) / 2.0, (hi - lo) / 2.0
            distances.append((profile["mean_amount"] - center) / radius)
    if isinstance(nulls, dict) and _has(nulls, "customer_id"):
        limit = b.get("null_rate_max")
        if limit:
            distances.append(nulls["customer_id"] / limit)
    if _has(profile, "staleness_min"):
        limit = b.get("staleness_min_max")
        if limit:
            distances.append(profile["staleness_min"] / limit)
    composite = math.sqrt(sum(x * x for x in distances)) if distances else 0.0
    if signals or composite > 2.0:
        reason = "; ".join(signals) if signals else "combined batch signals exceed baseline"
        return _alert(pillar, reason)
    return Verdict(alert=False, pillar=pillar)


def check_contract_checkpoint(payload, ctx):
    pillar = "contracts"
    result = ctx.tools.contract_diff(payload.get("contract_id"), payload.get("checkpoint_batch_id"))
    if not isinstance(result, dict) or "error" in result:
        return Verdict(alert=False, pillar=pillar, reason="contract diff unavailable")
    violations = result.get("violations") or []
    if violations:
        return _alert(pillar, "contract violations: " + ", ".join(str(v) for v in violations))
    delay = result.get("freshness_delay_min")
    max_delay = ctx.baseline.get("freshness_delay_max_min", math.inf)
    if delay is not None and delay > max_delay:
        return _alert(pillar, "freshness delay above baseline")
    return Verdict(alert=False, pillar=pillar)


def check_lineage_run(payload, ctx):
    pillar = "lineage"
    result = ctx.tools.lineage_graph_slice(payload.get("run_id"))
    if not isinstance(result, dict) or "error" in result:
        return Verdict(alert=False, pillar=pillar, reason="lineage slice unavailable")
    if result.get("actual_downstream_count") == 0:
        return _alert(pillar, "lineage output has no downstream consumers")
    # The expected upstream set is part of the event metadata when supplied;
    # avoid hardcoding a particular model's graph into the detector.
    expected = payload.get("expected_upstream")
    actual = result.get("actual_upstream")
    if expected and actual is not None and not set(expected).issubset(set(actual)):
        return _alert(pillar, "required upstream dependency missing")
    duration = result.get("duration_ms")
    if duration is not None and duration > ctx.baseline.get("lineage_duration_ms_max", math.inf):
        return _alert(pillar, "lineage runtime above baseline")
    return Verdict(alert=False, pillar=pillar)


def check_feature_materialization(payload, ctx):
    pillar = "ai_infra"
    ref = payload.get("batch_id")
    result = ctx.tools.feature_drift(payload.get("feature_view"), ref)
    if not isinstance(result, dict) or "error" in result:
        return Verdict(alert=False, pillar=pillar, reason="feature drift measurement unavailable")
    shift = result.get("mean_shift_sigma")
    # The published clean bound is 0.4095, while clean stream variation can
    # approach 0.472; 0.65 leaves margin for clean noise and catches clear skew.
    threshold = max(0.65, ctx.baseline.get("feature_mean_shift_sigma_max", 0.0))
    if shift is not None and shift > threshold:
        return _alert(pillar, "feature serving mean shifted from training")
    return Verdict(alert=False, pillar=pillar)


def check_embedding_batch(payload, ctx):
    pillar = "ai_infra"
    result = ctx.tools.embedding_drift(payload.get("corpus"), payload.get("chunk_batch_id"))
    if not isinstance(result, dict) or "error" in result:
        return Verdict(alert=False, pillar=pillar, reason="embedding measurement unavailable")
    shift = result.get("centroid_shift")
    age = result.get("avg_doc_age_days")
    if shift is not None and shift > ctx.baseline.get("embedding_centroid_shift_max", math.inf):
        return _alert(pillar, "embedding centroid shift above baseline")
    if age is not None and age > ctx.baseline.get("corpus_avg_doc_age_days_max", math.inf):
        return _alert(pillar, "corpus document age above baseline")
    return Verdict(alert=False, pillar=pillar)
