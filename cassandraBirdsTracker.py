import random
import time
from tqdm import tqdm
from datetime import date, datetime

from cassandra.cluster import Cluster, ResultSet
from cassandra.cluster import Session
from cassandra import ConsistencyLevel


KEYSPACE = "trackbirds"
TABLE = "birds_locations_by_date"


def get_all_bird_ids(session: Session, table: str):
    cql_get = f"""
SELECT DISTINCT bird_id, bucket_date FROM {table};
    """
    statement = session.prepare(cql_get)
    statement.consistency_level = ConsistencyLevel.ALL
    result: ResultSet = session.execute(statement)
    bird_ids = sorted({row.bird_id for row in result})
    return bird_ids


def get_info_of_bird(
    session: Session,
    table: str,
    bird_id: str,
    bucket_date: date,
    limit: int = 1,
    trace: bool = False,
):
    cql_insert = f"""
SELECT * FROM {table}
WHERE bird_id = ?
AND bucket_date = ?
LIMIT ?
"""
    values = [bird_id, bucket_date, limit]
    statement = session.prepare(cql_insert)
    statement.consistency_level = ConsistencyLevel.QUORUM
    return session.execute(statement, values, trace=trace)


def track_birds(
    session: Session,
    table: str,
    period_of_track_in_secodns: int = 60,
    amount_of_track: int = 5,
    keyspace: str = None,
):
    bird_list = get_all_bird_ids(session, table)
    bird_to_track = random.choice(bird_list)

    current_time = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    result_file = open(f"result_txt_{current_time}.txt", "w+")
    trace_file = open(f"track_select_{current_time}.txt", "w+")

    for _ in tqdm(range(amount_of_track)):
        for bird_id in bird_list:
            trace = bird_id is bird_to_track
            bucket_date = date.today()
            res = get_info_of_bird(
                session,
                table,
                bird_id,
                bucket_date,
                trace=trace,
                keyspace=keyspace,
            )

            if trace:
                trace_file.write(f"{res.get_query_trace()}\n")
                trace_file.flush()

            result_file.write(f"{str(res.all())}\n")
            result_file.flush()

        time.sleep(period_of_track_in_secodns)


def use_keyspace(session: Session, keyspace: str):
    session.execute(f"USE {keyspace}")


def connect_to_cluster():
    cluster = Cluster(
        contact_points=["localhost"],
        port=9042,
    )
    return cluster, cluster.connect(KEYSPACE)


def main():
    cluster, session = connect_to_cluster()
    track_birds(
        session, TABLE, period_of_track_in_secodns=20, amount_of_track=10
    )

    session.shutdown()


if __name__ == "__main__":
    main()
