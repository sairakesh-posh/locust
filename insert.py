from locust import FastHttpUser, task, constant, LoadTestShape
import random
import csv

# --- File Paths ---
BUCKET1_FILE = "/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket1.csv"
BUCKET2_FILE = "/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket2.csv"

# --- Configuration ---
BATCH_SIZE = 500
NUM_PREGENERATED_PAYLOADS = 100
ENDPOINT = "/entity-scores/reactive/models/2.0.1/namespaces/69e2029fb6a49b1efcaf2da0/batchInsert"

ALL_DATA = []

# --- Load Data ---
def load_data(filepath):
    count = 0
    with open(filepath, "r", encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) >= 2:
                # Store as a tuple of (id, score)
                ALL_DATA.append((row[0], row[1]))
                count += 1
    return count

print("Loading data from buckets...")
b1_count = load_data(BUCKET1_FILE)
b2_count = load_data(BUCKET2_FILE)
print(f"Loaded {b1_count} from Bucket 1 and {b2_count} from Bucket 2.")
print(f"Total pool size: {len(ALL_DATA)} IDs.")

# --- Payload Generation ---
def generate_batch_payload(size):
    # Randomly select 'size' number of tuples from our combined pool
    sampled_items = random.sample(ALL_DATA, size)
    
    # Build the dictionary mapping post_id -> score
    scores_map = {}
    for post_id, score in sampled_items:
        scores_map[post_id] = str(score)
        
    return {
        "scores": scores_map
    }

print(f"Pre-generating {NUM_PREGENERATED_PAYLOADS} payloads to maximize Locust performance...")
PRE_GENERATED_PAYLOADS = [generate_batch_payload(BATCH_SIZE) for _ in range(NUM_PREGENERATED_PAYLOADS)]

def get_payload():
    return random.choice(PRE_GENERATED_PAYLOADS)
    
class BatchInsertService(FastHttpUser):
    host = "https://stage-entity-score-api.aws.goshd.net"
    wait_time = constant(1)
    
    @task
    def load_test(self):
        data = get_payload()
        with self.client.post(
            ENDPOINT, 
            json=data,
            name="batchInsert",
            headers={"Content-Type": "application/json"},
            catch_response=True
            ) as response:
                if response.status_code not in (200, 201, 202): 
                    response.failure(f"Failed: {response.status_code} - {response.text}")
                else:
                    response.success()