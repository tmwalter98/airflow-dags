"""Moynihan Train Hall board workflow.

Scrapes the live departures/arrivals board from moynihantrainhall.nyc and
persists a snapshot to MongoDB, then reads the snapshot back. That final
read step is the workflow's entrypoint: it's the same query performed by
`GET /trains/moyhnihan` in the API service (see dags/trains_old.py), so
this DAG effectively produces what that endpoint serves.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

MONGO_CONN_ID = "mongo_trains"
MONGO_DB = "trains"
MONGO_COLLECTION = "moynihan_board"


@dag(
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["trains", "moynihan", "scrape"],
    default_args=default_args,
    doc_md=__doc__,
)
def moynihan_trains():
    @task()
    def fetch_board() -> list[dict]:
        """Scrape and parse the live train board (mirrors `fetch_moynihan_board`)."""
        import re
        from datetime import date, datetime

        import httpx
        import pandas as pd
        import pytz
        from bs4 import BeautifulSoup

        class MoynihanTrainHall(httpx.Client):
            def __init__(self):
                super().__init__(
                    base_url="https://moynihantrainhall.nyc",
                    timeout=30.0,
                    headers={
                        "accept": "application/json, text/javascript, */*; q=0.01",
                        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "origin": "https://moynihantrainhall.nyc",
                        "referer": "https://moynihantrainhall.nyc/transportation/",
                        "user-agent": (
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
                        ),
                        "x-requested-with": "XMLHttpRequest",
                    },
                    event_hooks={"response": [self._raise_for_status]},
                )

            def _raise_for_status(self, response: httpx.Response) -> None:
                response.raise_for_status()

            def get_train_board(self) -> dict[str, str]:
                res = self.post("/wp-admin/admin-ajax.php", data={"action": "ajax_amtrak_refresh"})
                return res.json()

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

        with MoynihanTrainHall() as client:
            train_board_res = client.get_train_board()

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
    def save_to_mongo(records: list[dict]) -> None:
        """Upsert the fetched snapshot into Mongo (mirrors `save_moynihan_to_mongo`)."""
        from datetime import datetime

        import pytz
        from airflow.providers.mongo.hooks.mongo import MongoHook

        hook = MongoHook(mongo_conn_id="mongodb_trains")
        client = hook.get_conn()
        col = client[MONGO_DB][MONGO_COLLECTION]
        fetched_at = datetime.now(tz=pytz.utc)
        docs = [{"fetched_at": fetched_at.isoformat(), **r} for r in records]
        if docs:
            col.insert_many(docs)

    @task()
    def get_moynihan_status() -> list[dict]:
        from airflow.providers.mongo.hooks.mongo import MongoHook

        hook = MongoHook(mongo_conn_id="mongodb_trains")
        client = hook.get_conn()
        col = client[MONGO_DB][MONGO_COLLECTION]
        latest = col.find_one(sort=[("fetched_at", -1)])
        if not latest:
            return []
        cursor = col.find({"fetched_at": latest["fetched_at"]}, {"_id": 0})
        return list(cursor)

    save_to_mongo(fetch_board()) >> get_moynihan_status()


moynihan_trains()
