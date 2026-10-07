import logging

from np3_alert_detector import RuleBasedAlertDetector


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


INPUT_FILE = (
    "curated_data_all/grid_hour_activity.csv"
)

OUTPUT_FILE = (
    "curated_data_all/activity_alerts.csv"
)


detector = RuleBasedAlertDetector(
    file_path=INPUT_FILE,

    # Initial operational thresholds
    high_multiplier=1.50,
    spike_multiplier=1.50,
    drop_multiplier=0.50
)


# 1. Load NP2 grid/hour table
detector.load_data()

# 2. Validate
detector.validate_input()

# 3. Select data-driven activity floor
detector.choose_activity_floor()

# 4. Build leave-one-out daily baseline
detector.build_baseline()


detector.build_baseline()

incomplete_days = (
    detector.validate_daily_coverage()
)

# 5. Remove low-activity grid-days from alert eligibility
detector.apply_activity_floor()

# 6. Evaluate three rules
detector.apply_rules()

# 7. Create alert records
alerts = detector.create_alert_records()

# 8. Compute summary
summary = detector.compute_summary()

# 9. Export
detector.export_alerts(
    OUTPUT_FILE
)

# 10. Operational summary
detector.operational_summary()