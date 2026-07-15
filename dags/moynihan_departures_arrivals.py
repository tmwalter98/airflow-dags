"""Moynihan Train Hall board workflow.

Scrapes the live departures/arrivals board from moynihantrainhall.nyc and
persists a snapshot to MongoDB, then reads the snapshot back. That final
read step is the workflow's entrypoint: it's the same query performed by
`GET /trains/moyhnihan` in the API service (see dags/trains_old.py), so
this DAG effectively produces what that endpoint serves.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task

MONGO_CONN_ID = "mongo_trains"
MONGO_DB = "trains"
MONGO_COLLECTION = "moynihan_board"
UPDATE_KEYS = ["board", "train_number", "train_name", "destination", "time", "status", "track"]


@dag(
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["trains", "moynihan", "scrape"],
)
def moynihan_departures_arrivals():
    @task()
    async def fetch_board() -> list[dict]:
        """Scrape and parse the live train board."""
        import re
        from datetime import date, datetime

        import pandas as pd
        import pytz
        from bs4 import BeautifulSoup
        from clients.moynihan_webdriver import MoynihanTrainHallWebDriver

        def parse_board(html: str) -> pd.DataFrame:
            time_regex = re.compile(r"^\d{1,2}:\d{2} (AM|PM)$")

            soup = BeautifulSoup(html, "html.parser")
            train_numbers = [e.get_text().strip() for e in soup.find_all("span", class_="train-number")]
            train_names = [e.get_text().strip() for e in soup.find_all("span", class_="train-name")]
            destinations = [e.get_text().strip() for e in soup.find_all("span", class_="pill-destination")]
            statuses = [e.get_text().strip() for e in soup.find_all("span", class_="pill-status")]
            tracks = [e.get_text().strip() for e in soup.find_all("td", class_="track-cell")]
            times = [e.get_text().strip() for e in soup.find_all("td", string=time_regex)]

            df = pd.DataFrame(
                {
                    "train_number": train_numbers,
                    "train_name": train_names,
                    "destination": destinations,
                    "time": times,
                    "status": statuses,
                    "track": tracks,
                }
            )
            current_date = date.today()
            parsed_time = pd.to_datetime(df["time"], format="%I:%M %p", utc=False).apply(
                lambda t: t.replace(year=current_date.year, month=current_date.month, day=current_date.day)
            )
            df["time"] = parsed_time.dt.tz_localize("America/New_York").dt.tz_convert("UTC").astype(str)
            return df

        driver = MoynihanTrainHallWebDriver()
        train_board_res = await driver.check_boards()

        last_updated = datetime.strptime(train_board_res["lastUpdate"], "%I:%M %p").replace(
            year=date.today().year, month=date.today().month, day=date.today().day
        )
        last_updated = pytz.timezone("America/New_York").localize(last_updated).astimezone(pytz.utc)

        departures_df = parse_board(train_board_res["departures"])
        departures_df.insert(0, "board", "DEPARTURES")
        arrivals_df = parse_board(train_board_res["arrivals"])
        arrivals_df.insert(0, "board", "ARRIVALS")
        boards_df = pd.concat([departures_df, arrivals_df], ignore_index=True)
        boards_df.insert(0, "last_updated", last_updated)
        boards_df["last_updated"] = boards_df["last_updated"].astype(str)
        boards_df.sort_values(by=["time"], inplace=True)
        return boards_df.to_dict(orient="records")

    @task()
    async def save_to_mongo(records: list[dict]) -> None:
        """Upsert the fetched snapshot into Mongo (mirrors `save_moynihan_to_mongo`)."""
        from datetime import datetime

        import pytz
        from airflow.providers.mongo.hooks.mongo import MongoHook
        from pymongo import UpdateOne

        hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
        client = hook.get_conn()
        col = client[MONGO_DB][MONGO_COLLECTION]
        updated_at = datetime.now(tz=pytz.utc)

        ops = []
        for e in records:
            u = UpdateOne(
                filter={k: e[k] for k in UPDATE_KEYS},
                update={"$set": {**e, "updated_at": updated_at.isoformat()}},
            )
            ops.append(u)

        if ops:
            col.bulk_write(ops, ordered=False)

    @task()
    async def get_moynihan_status() -> list[dict]:
        from airflow.providers.mongo.hooks.mongo import MongoHook

        hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
        client = hook.get_conn()
        col = client[MONGO_DB][MONGO_COLLECTION]
        latest = col.find_one(sort=[("updated_at", -1)])
        if not latest:
            return []
        cursor = col.find({"updated_at": latest["updated_at"]}, {"_id": 0})
        return list(cursor)

    save_to_mongo(fetch_board()) >> get_moynihan_status()


moynihan_departures_arrivals()
