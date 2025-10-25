import os
import random
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

PERIOD_FOR_UPDATE_IN_SECONDS = os.getenv("PERIOD_FOR_UPDATE_IN_SECONDS")
NUMBER_OF_UPDATE_CYCLES = os.getenv("NUMBER_OF_UPDATE_CYCLES")
NUMBER_OF_BIRDS_TO_SIMULATE = os.getenv("NUMBER_OF_BIRDS_TO_SIMULATE")


def create_keyspace(session: Session, keyspace: str) -> ResultSet:
    cql_create_keyspace = f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH REPLICATION = {{
    'class':'SimpleStrategy',
    'replication_factor' : 3
}};
"""
    statement = session.prepare(cql_create_keyspace)
    statement.consistency_level = ConsistencyLevel.ALL

    session.execute(statement, trace=True)


def create_table(session: Session, table: str):
    cql_create_table = f"""
CREATE TABLE IF NOT EXISTS {table}(
bird_id	        TEXT,
bucket_date	    DATE,
ts			    TimeUUID,
species		    TEXT,
latitude		DOUBLE,
longitude		DOUBLE,
PRIMARY KEY ((bird_id, bucket_date), ts)
) WITH CLUSTERING ORDER BY (
ts DESC);
    """
    statement = session.prepare(cql_create_table)
    statement.consistency_level = ConsistencyLevel.QUORUM
    session.execute(statement)
    session.execute(cql_create_table)


def init_database():
    cluster = Cluster(
        contact_points=["localhost"],
        port=9042,
    )
    session = cluster.connect()
    create_keyspace(session, KEYSPACE)
    session.shutdown()

    session = cluster.connect(KEYSPACE)
    create_table(session, TABLE)
    session.shutdown()
    cluster.shutdown()


def insert_into(
    session: Session,
    table: str,
    bird_id: str,
    bucket_date: date,
    latitude: float,
    longitude: float,
    species: str,
    ts: str = None,
    trace: bool = False,
) -> ResultSet:
    cql_insert = f"""
INSERT INTO {table}(
  bird_id,
  bucket_date,
  ts,
  latitude,
  longitude,
  species
)
VALUES (
    ?,
    ?,
    {"now()" if ts is None else "?"},
    ?,
    ?,
    ?);
    """

    values = [bird_id, bucket_date, latitude, longitude, species]
    if ts is not None:
        values.insert(2, ts)
    statement = session.prepare(cql_insert)
    statement.consistency_level = ConsistencyLevel.QUORUM
    return session.execute(statement, values, trace=trace)


def random_step(lat, lon, step_deg=0.01):
    # random small change within ±step_deg
    dlat = random.uniform(-step_deg, step_deg)
    dlon = random.uniform(-step_deg, step_deg)

    # apply
    new_lat = lat + dlat
    new_lon = lon + dlon

    # clamp latitude
    if new_lat > 90:
        new_lat = 180 - new_lat  # reflect back in
    elif new_lat < -90:
        new_lat = -180 - new_lat  # reflect back in
    # wrap longitude (like a globe)
    if new_lon > 180:
        new_lon -= 360
    elif new_lon < -180:
        new_lon += 360

    return new_lat, new_lon


def simulate_birds(
    session: Session,
    table: str,
    number_of_birds: int = 10,
    period_for_update_in_seconds: float = 5,
    number_of_update_cycles: int = 20,
):
    bird_ids = [f"bird_{idx}" for idx in range(1, number_of_birds + 1)]
    bird_to_trace = "bird_4"  # random.choice(bird_ids)
    species_options = [
        "American Robin",
        "American Crow",
        "European Starling",
        "Mourning Dove",
        "Rock Pigeon",
    ]

    birds_info = [
        {
            "bird_id": bird_id,
            "species": random.choice(species_options),
            "latitude": random.uniform(-180, 180),
            "longitude": random.uniform(-90, 90),
        }
        for bird_id in bird_ids
    ]

    current_time = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    trace_file = open(f"trace_insert_{current_time}.txt", "w+")

    for idx in tqdm(
        range(number_of_update_cycles), desc="updating bird locations"
    ):
        if idx != 0:
            time.sleep(period_for_update_in_seconds)

        random.shuffle(birds_info)
        for bird in birds_info:
            current_date = date.today()
            bird["latitude"], bird["longitude"] = random_step(
                bird["latitude"], bird["longitude"]
            )

            result = insert_into(
                session,
                table,
                bird_id=bird["bird_id"],
                bucket_date=current_date,
                latitude=bird["latitude"],
                longitude=bird["longitude"],
                species=bird["species"],
                trace=True if bird["bird_id"] == bird_to_trace else False,
            )

            if bird["bird_id"] == bird_to_trace:
                trace = result.get_query_trace()
                for e in trace.events:
                    trace_file.write(f"{e.source_elapsed} - {e.description}\n")
                trace_file.write("\n\n")
                trace_file.flush()

        time.sleep(period_for_update_in_seconds)


def connect_to_cluster():
    cluster = Cluster(
        contact_points=CONTACT_POINTS,
        port=PORT,
    )
    return cluster, cluster.connect(KEYSPACE)


def main():
    init_database()
    cluster, session = connect_to_cluster()
    simulate_birds(
        session,
        TABLE,
        number_of_birds=NUMBER_OF_BIRDS_TO_SIMULATE,
        period_for_update_in_seconds=PERIOD_FOR_UPDATE_IN_SECONDS,
        number_of_update_cycles=NUMBER_OF_UPDATE_CYCLES,
    )
    session.shutdown()


if __name__ == "__main__":
    main()
