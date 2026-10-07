# C3 — Context Engineering Checklist

## Purpose

Use this checklist when preparing network evidence for a long-context
Claude investigation.

The objective is to provide Claude with the most relevant evidence
while reducing unnecessary context and preserving important
uncertainty.

---

## 1. Collect the Required Evidence

Before asking Claude to investigate an incident, collect:

- [ ] Current grid activity
- [ ] Current grid features
- [ ] Anomaly score and classification
- [ ] ML risk score and risk level
- [ ] Grid location
- [ ] Recent historical activity
- [ ] Prior alerts
- [ ] Pipeline/data-quality status

---

## 2. Separate Current and Historical Evidence

Organize evidence into:

- [ ] CURRENT EVIDENCE
- [ ] HISTORICAL EVIDENCE
- [ ] UNCERTAINTY

Do not mix historical observations with the current network state.

---

## 3. Summarize Long Historical Evidence

Before inserting historical data into Claude's active context:

- [ ] Determine the number of historical observations.
- [ ] Calculate the historical average.
- [ ] Identify minimum activity.
- [ ] Identify maximum activity.
- [ ] Preserve timestamps for important historical points.
- [ ] Identify relevant prior alerts.
- [ ] Remove redundant hourly records when they do not add
      decision-making value.

---

## 4. Preserve Decision-Relevant Evidence

Do not remove information simply because it is old.

Retain:

- [ ] Current activity value
- [ ] Anomaly classification
- [ ] ML risk result
- [ ] Historical range
- [ ] Important historical peaks or lows
- [ ] Prior alerts
- [ ] Data-quality information relevant to interpretation
- [ ] Model limitations

---

## 5. Avoid Dumping Irrelevant Context

Before sending context to Claude:

- [ ] Remove unrelated grids.
- [ ] Remove unrelated alerts.
- [ ] Remove duplicate information.
- [ ] Remove unnecessary raw historical records.
- [ ] Remove implementation/debug information.
- [ ] Keep only information relevant to the investigation question.

---

## 6. Preserve Uncertainty

Claude must not be forced to produce a confident answer when
the evidence is insufficient.

Check that:

- [ ] Limited historical coverage is disclosed.
- [ ] Absence of an alert is not treated as proof that an event
      never occurred.
- [ ] ML predictions are described as predictive evidence.
- [ ] Missing evidence is explicitly identified.
- [ ] Conclusions do not exceed what the evidence supports.

---

## 7. Prevent Unsupported Conclusions

Claude should:

- [ ] Use only supplied evidence.
- [ ] Avoid inventing network events.
- [ ] Avoid inventing historical alerts.
- [ ] Avoid treating ML predictions as confirmed events.
- [ ] Distinguish observations from interpretations.
- [ ] State when the available evidence is insufficient.

---

## 8. Validate the Curated Context

Before using the curated context:

- [ ] Confirm the grid ID is correct.
- [ ] Confirm the investigation timestamp.
- [ ] Confirm current activity values.
- [ ] Confirm anomaly classification.
- [ ] Confirm ML risk values.
- [ ] Confirm historical summary calculations.
- [ ] Confirm prior-alert information.
- [ ] Confirm that important uncertainty has not been removed.

---

## 9. Compare Context Strategies

For C3 experiments:

- [ ] Run the investigation with Dump Everything context.
- [ ] Run the investigation with Curated Context.
- [ ] Use the same investigation question.
- [ ] Compare the operational conclusion.
- [ ] Compare historical interpretation.
- [ ] Compare uncertainty handling.
- [ ] Determine whether irrelevant context affected answer quality.

---

## 10. Preferred Strategy

When the same decision-relevant evidence can be represented more
efficiently:

> Prefer curated context over a raw context dump.

The curated context should reduce unnecessary information while
preserving the evidence required for a safe operational conclusion.

---

## C3 Success Criteria

The context-engineering process is successful when:

- [ ] Claude receives relevant current evidence.
- [ ] Historical evidence is summarized.
- [ ] Current and historical evidence are clearly separated.
- [ ] Uncertainty is explicitly represented.
- [ ] Irrelevant context is removed.
- [ ] No unsupported conclusions are introduced.
- [ ] The curated answer is at least as useful as the raw-context
      answer.
- [ ] The investigation remains grounded in the underlying network
      evidence.