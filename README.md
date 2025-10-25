# Gil Yair Yamin - 207027053

Disclosure, chatGPT was used to create this readme file.

# 🐦 Cassandra Birds Tracker

This project simulates and tracks the movement of birds using a **Cassandra database**.  
It includes two main Python scripts:

- `cassandraBirdsClient.py` — simulates bird location updates and inserts them into Cassandra.  
- `cassandraBirdsTracker.py` — periodically queries and traces birds’ latest locations.

## ⚙️ Prerequisites
### 1. Install Cassandra
You must have **Apache Cassandra** installed and running locally or remotely.

- Default contact point: `localhost`
- Default port: `9042`

### 2. Install Python dependencies
Use a virtual environment if possible.

```bash
pip install -r requirements.txt
```

The dependencies are:
- `python-dotenv` — loads environment variables from `.env`
- `cassandra-driver` — Python driver for Cassandra

## 🔧 Configuration (.env)

All connection parameters and timing configurations are defined in the `.env` file:

```env
KEYSPACE="trackbirds"
TABLE="birds_locations_by_date"

CONTACT_POINTS="localhost"
PORT=9042

PERIOD_FOR_UPDATE_IN_SECONDS=60
NUMBER_OF_UPDATE_CYCLES=2
NUMBER_OF_BIRDS_TO_SIMULATE=10

PERIOD_FOR_TRACKING_IN_SECONDS=60
NUMBER_OF_TRACKING_CYCLES=2
```

- **KEYSPACE** — Cassandra keyspace name  
- **TABLE** — Table to store bird data  
- **CONTACT_POINTS** — Comma-separated list of Cassandra node IPs or hostnames  
- **PORT** — Cassandra connection port  

- **PERIOD_FOR_UPDATE_IN_SECONDS** — Interval between insert updates. 
- **NUMBER_OF_UPDATE_CYCLES** — Number of insert iterations.
- **NUMBER_OF_BIRDS_TO_SIMULATE** — Number of birds to simulate.

- **PERIOD_FOR_TRACKING_IN_SECONDS** — Interval between tracking cycles.
- **NUMBER_OF_TRACKING_CYCLES** — Number of tracking iterations.


## 🧩 Script 1: cassandraBirdsClient.py (Data Generator)

Notice: each update cycle does an update *on all birds*. So if we have 10 birds and 2 cycles, it will do 20 updates overall.
This script simulates bird movement and writes GPS data into Cassandra.

### Features
- Creates keyspace and table if not existing  
- Simulates random bird motion (latitude/longitude drift)  
- Inserts position updates with timestamps (`TimeUUID`)  
- Logs query traces for one selected bird  

### Usage

```bash
python cassandraBirdsClient.py
```

This will:
1. Connect to Cassandra (using `.env` parameters)  
2. Create a keyspace and table (`birds_locations_by_date`) if they don’t exist  
3. Insert new randomized location data for multiple birds  
4. Save tracing information to a log file named `trace_insert_<timestamp>.txt`

Example log snippet:
```
34 - Parsing insert statement
78 - Sending mutation message
120 - Mutation applied
```

---

## 🧭 Script 2: cassandraBirdsTracker.py (Data Tracker)

This script periodically queries bird data from Cassandra to simulate real-time tracking.

### Features
- Queries distinct bird IDs from Cassandra  
- Fetches the latest location data per bird  
- Performs tracing for a single “target” bird  
- Logs query and trace data into timestamped result files  

### Usage

```bash
python cassandraBirdsTracker.py
```

This will:
1. Connect to Cassandra  
2. Retrieve bird IDs  
3. Loop through tracking cycles, fetching bird locations every `PERIOD_FOR_TRACKING_IN_SECONDS`  
4. Save results and traces into:
   - `result_txt_<timestamp>.txt`
   - `track_select_<timestamp>.txt`

---

## 📊 Example Workflow

1. **Start Cassandra locally**:
   ```bash
   cassandra -f
   ```

2. **Run the client script** to populate data:
   ```bash
   python cassandraBirdsClient.py
   ```

3. **Run the tracker script** to monitor birds:
   ```bash
   python cassandraBirdsTracker.py
   ```

4. Check generated text files in your working directory for query results and trace logs.

---

## 🧠 Notes

- Both scripts use `tqdm` for progress visualization.  
- All queries use `ConsistencyLevel.QUORUM` to ensure reliable reads/writes.  
- Default consistency and replication are suitable for local testing, not production.  
- You can modify the `.env` file to point to a real Cassandra cluster.

---

## 🪶 Example Output Files

```
result_txt_2025-10-26_01.00.00.txt
track_select_2025-10-26_01.00.00.txt
trace_insert_2025-10-26_01.00.00.txt
```

Each line represents a record or trace event generated during simulation and tracking.

---

## ✅ Summary

| Script | Purpose | Output |
|--------|----------|--------|
| **cassandraBirdsClient.py** | Simulate and insert bird movement data | `trace_insert_*.txt` |
| **cassandraBirdsTracker.py** | Query and trace bird data | `result_txt_*.txt`, `track_select_*.txt` |
| **.env** | Configuration for Cassandra & timing | — |
| **requirements.txt** | Dependency list | — |

