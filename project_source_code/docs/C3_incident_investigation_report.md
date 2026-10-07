# C3 — Long-Context Incident Investigation Report

## 1. Investigation Overview

**Grid ID:** 4821

**Investigation Timestamp:** 2013-11-07 23:00

**Investigation Question:**

> Has this abnormal activity pattern happened before, and can the
> current data be trusted?

This investigation demonstrates long-context incident analysis using
two different context strategies:

1. Dump Everything
2. Curated Context

The same underlying network evidence was provided to Claude using
both approaches.

---

## 2. Current Evidence

At 23:00 on 2013-11-07, Grid 4821 reported:

| Metric | Value |
|---|---:|
| Total Activity | 254.97 |
| Internet Activity | 232.86 |
| Calls | 4.01 |
| SMS | 18.09 |

### Anomaly Assessment

| Field | Result |
|---|---|
| Anomaly Flag | False |
| Direction | NORMAL |
| Anomaly Score | 0.3336 |
| Deviation | -33.36% |
| Historical Baseline | 382.63 |
| Recommended Action | NONE |

The anomaly service classifies the current activity as NORMAL and
within the historical range for the evaluated hour.

### ML Risk Assessment

| Field | Result |
|---|---|
| Risk Level | LOW |
| Risk Score | 10.0% |
| Model Version | ml3-decision-tree-v1 |

The ML risk score is a predictive operational indicator. It does not
by itself confirm network congestion or an actual anomaly.

---

## 3. Historical Evidence

The collected historical evidence contains 24 hourly activity
observations for Grid 4821.

### Historical Activity Summary

| Metric | Value |
|---|---:|
| Observations | 24 |
| Average Activity | 360.25 |
| Minimum Activity | 136.58 |
| Minimum Timestamp | 05:00 |
| Maximum Activity | 666.74 |
| Maximum Timestamp | 21:00 |
| Latest Activity | 254.97 |
| Latest Timestamp | 23:00 |

The current activity of 254.97 is within the observed 24-hour
historical range.

### Prior Alerts

No prior alerts were returned for Grid 4821 in the collected alert
evidence.

However, absence of prior alerts does not prove that the same
activity pattern never occurred.

The available historical evidence represents a 24-hour activity
window and is therefore insufficient to establish whether the exact
pattern occurred elsewhere in the grid's longer-term history.

### Historical Baseline

The anomaly assessment uses 6 historical observations for the
evaluated hour.

Because the baseline contains only a small number of observations,
the hour-specific normality assessment should be interpreted with
appropriate caution.

---

## 4. Uncertainty

The investigation contains several evidence limitations.

### Historical Coverage

The collected activity history covers 24 hourly observations.

This is sufficient to understand the recent activity pattern but is
not sufficient to prove whether the same pattern occurred over a
longer historical period.

### Prior Alerts

No prior alerts were returned for the grid.

This indicates that no prior alert was present in the collected
alert evidence, but it does not prove that the activity pattern
never occurred.

### ML Interpretation

The ML risk score represents a prediction for the defined ML target.

It should be treated as supporting evidence rather than proof of an
actual network event.

---

## 5. Dump Everything vs Curated Context

### 5.1 Dump Everything

The Dump Everything approach supplied the complete collected evidence
package to Claude.

This included all 24 hourly historical activity records.

The resulting Claude response was incomplete and stopped before
providing a complete investigation.

This demonstrates that providing more raw information does not
necessarily produce a better operational answer.

### 5.2 Curated Context

The Curated Context approach summarized historical information before
placing it into Claude's active context.

The 24 raw historical observations were reduced to decision-relevant
information such as:

- observation count;
- historical average;
- minimum activity;
- maximum activity;
- important timestamps;
- latest activity;
- prior alert information;
- anomaly baseline size; and
- ML interpretation.

The curated prompt explicitly separated:

- CURRENT EVIDENCE
- HISTORICAL EVIDENCE
- UNCERTAINTY

Claude produced a more complete and structured investigation using
the curated context.

---

## 6. Context-Engineering Evaluation

The experiment showed that the core operational conclusion remained
consistent between the two approaches.

Both approaches identified:

- NORMAL anomaly classification;
- LOW ML risk; and
- no evidence sufficient to establish a confirmed abnormal event.

However, the curated context produced a clearer investigation.

### Benefits observed

1. Historical evidence was easier to interpret.
2. Redundant raw records were removed.
3. Important historical statistics were preserved.
4. Current and historical evidence were clearly separated.
5. Uncertainty was explicitly represented.
6. Claude was less dependent on synthesizing every raw historical
   record.
7. The final response was more complete and operationally useful.

---

## 7. Final Investigation Conclusion

Based on the available evidence, Grid 4821 is currently classified as
NORMAL by the anomaly service, with LOW ML risk.

The available evidence does not establish that the same activity pattern
has definitely occurred before.

The absence of prior alerts cannot be interpreted as proof that the
pattern never occurred.

The available 24-hour history demonstrates that the current activity
falls within the observed range, but a larger historical search would
be required to determine whether the same pattern occurred over a
longer period.

### Operational Conclusion

> Grid 4821 does not currently show a confirmed abnormal activity
> condition in the supplied evidence. The activity is classified as
> NORMAL and the ML risk is LOW. Historical evidence shows that the
> current activity falls within the observed 24-hour range, but the
> available evidence is insufficient to determine whether the same
> pattern occurred over a longer historical period.

---

## 8. C3 Learning Outcome

The investigation demonstrates that effective context engineering is
not about sending the maximum amount of information to Claude.

Instead, the goal is to provide:

- relevant current evidence;
- summarized historical evidence;
- decision-relevant alerts;
- model interpretation; and
- explicit uncertainty.

The Dump Everything and Curated Context experiment demonstrated that
a smaller, structured context can produce a clearer and more complete
operational investigation while preserving the evidence required for
decision-making.