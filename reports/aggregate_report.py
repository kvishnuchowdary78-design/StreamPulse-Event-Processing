
import json
import sqlite3
from pathlib import Path

DATABASE = Path("data/output/streampulse.db")
REPORT_FILE = Path("reports/aggregate_summary.json")


def generate_report():
    connection = sqlite3.connect(DATABASE)
    connection.row_factory = sqlite3.Row

    try:
        # Overall processing aggregates
        overall = connection.execute("""
            SELECT
                COUNT(*) AS total_orders,
                COALESCE(SUM(amount), 0) AS total_revenue,
                COALESCE(AVG(amount), 0) AS average_order_value,
                COALESCE(SUM(is_late), 0) AS late_orders
            FROM processed_events
        """).fetchone()

        # Customer-level aggregates
        customer_rows = connection.execute("""
            SELECT
                customer_id,
                COUNT(*) AS order_count,
                SUM(amount) AS total_amount,
                ROUND(AVG(amount), 2) AS average_amount,
                SUM(is_late) AS late_orders
            FROM processed_events
            GROUP BY customer_id
            ORDER BY customer_id
        """).fetchall()

        # Processing quality counts
        rejected = connection.execute(
            "SELECT COUNT(*) FROM rejected_events"
        ).fetchone()[0]

        duplicates = connection.execute(
            "SELECT COUNT(*) FROM duplicate_events"
        ).fetchone()[0]

        report = {
            "overall": dict(overall),
            "data_quality": {
                "rejected_events": rejected,
                "duplicate_events": duplicates
            },
            "customer_summary": [
                dict(row) for row in customer_rows
            ]
        }

        REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

        with open(REPORT_FILE, "w", encoding="utf-8") as file:
            json.dump(report, file, indent=4)

        print("\n========== AGGREGATE REPORT ==========")
        print("Total orders:", overall["total_orders"])
        print("Total revenue:", round(overall["total_revenue"], 2))
        print(
            "Average order value:",
            round(overall["average_order_value"], 2)
        )
        print("Late orders:", overall["late_orders"])
        print("Rejected events:", rejected)
        print("Duplicate events:", duplicates)

        print("\nCustomer Summary:")
        for row in customer_rows:
            print(
                row["customer_id"],
                "| Orders:", row["order_count"],
                "| Revenue:", round(row["total_amount"], 2),
                "| Late:", row["late_orders"]
            )

        print("\nReport saved to:", REPORT_FILE)

    finally:
        connection.close()


if __name__ == "__main__":
    generate_report()
