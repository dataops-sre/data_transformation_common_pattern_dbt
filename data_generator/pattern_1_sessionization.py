import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import uuid

# Configuration
OUTPUT_DIR = "raw_events_data"
START_DATE = datetime(2026, 2, 18)  # 7 days ago from today (Feb 25, 2026)
DAYS = 7
POWER_USERS_COUNT = 20
INFREQUENT_USERS_COUNT = 80
SESSION_GAP_MINUTES = 30


def generate_events():
    np.random.seed(42)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_data = []

    # Define Users
    users = [
        {"id": f"user_power_{i}", "type": "power"} for i in range(POWER_USERS_COUNT)
    ] + [
        {"id": f"user_infrequent_{i}", "type": "infrequent"}
        for i in range(INFREQUENT_USERS_COUNT)
    ]

    for user in users:
        current_time = START_DATE + timedelta(hours=np.random.randint(0, 12))

        # Determine event count based on user type
        if user["type"] == "power":
            event_count = 2000
            # Power users have short gaps (stays in session) and occasional long gaps (new session)
            gap_weights = [0.95, 0.05]  # 5% chance of starting a new session
        else:
            event_count = np.random.randint(1, 101)
            gap_weights = [0.4, 0.6]  # 60% chance of a long gap

        for _ in range(event_count):
            all_data.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "user_id": user["id"],
                    "event_time": current_time,
                    "event_type": np.random.choice(
                        ["page_view", "click", "scroll", "add_to_cart"]
                    ),
                    "metadata": f"path_/{np.random.randint(1, 20)}",
                }
            )

            # Logic to increment time
            is_long_gap = np.random.choice([False, True], p=gap_weights)
            if is_long_gap:
                # 31 to 120 minutes gap (Guaranteed new session)
                minutes_to_add = np.random.randint(31, 120)
            else:
                # 1 to 10 minutes gap (Same session)
                minutes_to_add = np.random.randint(1, 11)

            current_time += timedelta(minutes=minutes_to_add)

            # Stop if we drift past the 7-day window
            if current_time > START_DATE + timedelta(days=DAYS):
                break

    # Create DataFrame
    df = pd.DataFrame(all_data)

    # Partitioning and Saving
    df["date"] = df["event_time"].dt.date
    unique_dates = df["date"].unique()

    print(f"Generating data for {len(unique_dates)} days...")

    for d in unique_dates:
        day_str = d.strftime("%Y-%m-%d")
        day_dir = os.path.join(OUTPUT_DIR, f"date={day_str}")
        os.makedirs(day_dir, exist_ok=True)

        day_df = df[df["date"] == d].drop(columns=["date"])
        # Shuffle to simulate out-of-order arrival within the day
        day_df = day_df.sample(frac=1).reset_index(drop=True)

        file_path = os.path.join(day_dir, "events.csv")
        day_df.to_csv(file_path, index=False)
        print(f"  Created: {file_path} ({len(day_df)} events)")


if __name__ == "__main__":
    generate_events()
    print("\nDataset generation complete. Ready for dbt/SQLMesh ingestion.")
