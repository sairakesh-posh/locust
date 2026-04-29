from locust import FastHttpUser, task, constant, LoadTestShape
import json, random, csv

# --- File Paths ---
BATCH1_FILE = "/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket1.csv"
BATCH2_FILE = "/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket2.csv"
BATCH3_FILE = '/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket3.csv'
BATCH4_FILE = '/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket4.csv'

BATCH1_DATA = []
BATCH2_DATA = []
BATCH3_DATA = []
BATCH4_DATA = []

HEADERS = {
    "Content-type": "application/json",
    "x-http-authorization": "Bearer eyJraWQiOiIwZDQ4ZmJmNC04NTAzLTQzMmYtYWUzMC1hOWZhMDQ4NGY3NzUiLCJhbGciOiJFUzI1NiJ9.eyJleHAiOjE3NzczNTQwMTh9.hSugrONc9aNv1xvsjCyfFAM9nazZlRCAHcOYkofO-4Ffc1yQKQul7E7to0Veu9FkhAgZLIyFnx5Yo2J-d69qHw"
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
load_data(BATCH3_FILE, BATCH3_DATA)
load_data(BATCH4_FILE, BATCH4_DATA)
print(f"Loaded {len(BATCH1_DATA)} from Bucket 1, {len(BATCH2_DATA)} from Bucket 2.")
print(f"Loaded {len(BATCH3_DATA)} from Bucket 3, {len(BATCH4_DATA)} from Bucket 4.")

# --- Configuration Patterns ---
batch_patterns = [
    (25, 25, 25, 25),   
    (50, 25, 15, 10),  
    (100, 0, 0, 0)     
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
def generate_payload(exact_n, multimodal_n, nofm_n, b1_pct, b2_pct, b3_pct, b4_pct):
    payload_size = exact_n + multimodal_n + nofm_n

    # Compute counts
    b1_count = int(payload_size * (b1_pct / 100.0))
    b2_count = int(payload_size * (b2_pct / 100.0))
    b3_count = int(payload_size * (b3_pct / 100.0))

    # Assign remainder to last bucket (avoids rounding mismatch)
    b4_count = payload_size - (b1_count + b2_count + b3_count)

    # Sample safely
    b1_sample = random.sample(BATCH1_DATA, b1_count) if b1_count > 0 else []
    b2_sample = random.sample(BATCH2_DATA, b2_count) if b2_count > 0 else []
    b3_sample = random.sample(BATCH3_DATA, b3_count) if b3_count > 0 else []
    b4_sample = random.sample(BATCH4_DATA, b4_count) if b4_count > 0 else []

    # Combine + shuffle
    combined_pool = b1_sample + b2_sample + b3_sample + b4_sample
    random.shuffle(combined_pool)

    # Split into retrieval sets
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
    b1_pct, b2_pct, b3_pct, b4_pct = SELECTED_BATCH_PATTERN

    PRE_GENERATED_PAYLOADS.append(
        generate_payload(
            exact_n, multimodal_n, nofm_n,
            b1_pct, b2_pct, b3_pct, b4_pct
        )
    )

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
