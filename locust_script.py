from locust import FastHttpUser, task, constant, LoadTestShape
import json, random, csv

# --- File Paths ---
BATCH1_FILE = "/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket1.csv"
BATCH2_FILE = "/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket2.csv"

BATCH1_DATA = []
BATCH2_DATA = []

HEADERS = {
    "Content-type": "application/json",
    "x-http-authorization": "Bearer eyJraWQiOiJkODBjOTI1OS0zYjM4LTQ0NDktODRiOS1kMTQ0Y2UwZDAyZjUiLCJhbGciOiJFUzI1NksifQ.eyJleHAiOjE3NzY3NTgxMjF9.YVRsNNoqFX57QJtRY25PEyOefs5QZuzPj9VNYCas_1dC3CaG7WY7gQ3goMCK1OhIoTOpb2uH09n04vmKRKaV5w"
}

# --- Load Data ---
def load_data(filepath, data_list):
    with open(filepath, "r", encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) >= 2:
                data_list.append({
                    "post_id": row[0],
                    "relevance_score": float(row[1]),
                })

print("Loading data from buckets...")
load_data(BATCH1_FILE, BATCH1_DATA)
load_data(BATCH2_FILE, BATCH2_DATA)
print(f"Loaded {len(BATCH1_DATA)} from Bucket 1, {len(BATCH2_DATA)} from Bucket 2.")

# --- Configuration Patterns ---
batch_patterns = [
    (50, 50),  
    (75, 25),  
    (100, 0)   
]

size_patterns = [
    (50, 50, 50),    
    (100, 100, 100), 
    (166, 167, 167), 
    (200, 200, 200), 
]

SELECTED_SIZE_PATTERN = size_patterns[2]    
SELECTED_BATCH_PATTERN = batch_patterns[0]  

# --- Payload Generation ---
def generate_payload(exact_n, multimodal_n, nofm_n, b1_pct, b2_pct):
    payload_size = exact_n + multimodal_n + nofm_n
    
    # Calculate exact counts safely (int cast rounds down, so we subtract for the remainder)
    b1_count = int(payload_size * (b1_pct / 100.0))
    b2_count = payload_size - b1_count 
    
    # Sample from each pool independently
    b1_sample = random.sample(BATCH1_DATA, b1_count) if b1_count > 0 else []
    b2_sample = random.sample(BATCH2_DATA, b2_count) if b2_count > 0 else []
    
    # Combine and shuffle to distribute the batch logic randomly across retrieval sets
    combined_pool = b1_sample + b2_sample
    random.shuffle(combined_pool)
    
    # Slice the shuffled pool into the required sizes
    exact_posts = combined_pool[0:exact_n]
    multimodal_posts = combined_pool[exact_n:exact_n + multimodal_n]
    nofm_posts = combined_pool[exact_n + multimodal_n:]
    
    return {
        "model_version": {
            "post_score": "2.0.1",
            "rescore": "1.0.0"
        },
        "redis_ops": "reactive",
        "retrieval_sets": [
            {
                "retrieval_cycle": "lexical.exact",
                "posts": exact_posts
            },
            {
                "retrieval_cycle": "lexical.multimodal.ensemble",
                "posts": multimodal_posts
            },
            {
                "retrieval_cycle": "lexical.nofm",
                "posts": nofm_posts
            }
        ]
    }

PRE_GENERATED_PAYLOADS = []
for _ in range(200):
    exact_n, multimodal_n, nofm_n = SELECTED_SIZE_PATTERN
    b1_pct, b2_pct = SELECTED_BATCH_PATTERN
    PRE_GENERATED_PAYLOADS.append(generate_payload(exact_n, multimodal_n, nofm_n, b1_pct, b2_pct))

def get_payload():
    return random.choice(PRE_GENERATED_PAYLOADS)

# --- Locust Test Mechanics ---
# class StepLoadShape(LoadTestShape):
#     stages = [
#         {"duration": 180, "users": 10, "spawn_rate": 2},    
#         # {"duration": 360, "users": 30, "spawn_rate": 5},  
#         {"duration": 540, "users": 50, "spawn_rate": 5},
#         # {"duration": 720, "users": 75, "spawn_rate": 5},
#         {"duration": 900, "users": 100, "spawn_rate": 10}
#     ]

#     def tick(self):
#         run_time = self.get_run_time()

#         for stage in self.stages:
#             if run_time < stage["duration"]:
#                 return (stage["users"], stage["spawn_rate"])

#         return None
    
class ReScoreService(FastHttpUser):
    host = "https://stage-search-reranker-api.aws.goshd.net"
    # wait_time = constant(0.6)
    run_time = 10
    
    @task
    def load_test(self):
        data = get_payload()
        with self.client.post(
            '/api/rerank', 
            json=data,
            name="rerank",
            headers=HEADERS,
            catch_response=True
            ) as response:
                if response.status_code != 200:
                    response.failure(f"Failed: {response.status_code}")