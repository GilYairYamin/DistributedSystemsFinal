import os
import time
from datetime import date, datetime

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ResultSet, Session
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
KEYSPACE = os.getenv("KEYSPACE")
TABLE = os.getenv("TABLE")
CONTACT_POINTS = os.getenv("CONTACT_POINTS").split(",")
PORT = os.getenv("PORT")

PERIOD_FOR_TRACKING_IN_SECONDS = os.getenv("PERIOD_FOR_TRACKING_IN_SECONDS")
NUMBER_OF_TRACKING_CYCLES = os.getenv("NUMBER_OF_TRACKING_CYCLES")


def get_all_bird_ids(session: Session, table: str):
    cql_get = f"""
SELECT DISTINCT bird_id, bucket_date FROM {table};
    """
    statement = session.prepare(cql_get)
    statement.consistency_level = ConsistencyLevel.QUORUM
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
) -> ResultSet:
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
    number_of_tracking_cycles: int = 5,
):
    bird_list = get_all_bird_ids(session, table)
    bird_to_trace = bird_list[3]  # random.choice(bird_list)
    print(f"trace - {bird_to_trace}")

    current_time = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    result_file = open(f"result_txt_{current_time}.txt", "w+")
    trace_file = open(f"track_select_{current_time}.txt", "w+")

    for idx in tqdm(range(number_of_tracking_cycles), desc="tracking birds"):
        if idx != 0:
            time.sleep(period_of_track_in_secodns)

        for bird_id in bird_list:
            trace = bird_id == bird_to_trace
            bucket_date = date.today()
            res: ResultSet = get_info_of_bird(
                session,
                table,
                bird_id,
                bucket_date,
                trace=trace,
            )

            if trace:
                trace_events = res.get_query_trace()
                for e in trace_events.events:
                    trace_file.write(f"{e.source_elapsed} - {e.description}\n")
                trace_file.write("\n\n")
                trace_file.flush()

            result_file.write(f"{str(res.all())}\n")
            result_file.flush()


def connect_to_cluster():
    cluster = Cluster(
        contact_points=CONTACT_POINTS,
        port=PORT,
    )
    return cluster, cluster.connect(KEYSPACE)


def main():
    cluster, session = connect_to_cluster()
    track_birds(
        session,
        TABLE,
        period_of_track_in_secodns=PERIOD_FOR_TRACKING_IN_SECONDS,
        number_of_tracking_cycles=NUMBER_OF_TRACKING_CYCLES,
    )
    session.shutdown()


if __name__ == "__main__":
    main()
