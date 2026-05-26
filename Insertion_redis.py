import asyncio
import aiohttp
import csv
import time

# --- Configuration ---
FILE_PATH = "/Users/sairakeshreddy/goshposh/locust/mock_scores.csv"
BASE_URL = "https://stage-entity-score-api.aws.goshd.net/entity-scores/models/stage-listing-quality-score-model-2026-04-27T15-38"

BATCH_SIZE = 2000
MAX_CONCURRENT_REQUESTS = 20
SKIP_ROWS = 109000000 

HEADERS = {
    "Content-Type": "application/json"
}

# --- Step 1: Create Namespace ---
async def create_namespace(session):
    url = f"{BASE_URL}/namespace"
    print("Creating / Fetching namespace...")

    async with session.post(url, data="") as response:
        resp_json = await response.json()

        if not resp_json.get("success"):
            raise Exception(f"Namespace creation failed: {resp_json}")

        namespace = resp_json["data"]["namespace"]
        print(f"✅ Namespace obtained: {namespace}")
        return namespace

MAX_RETRIES = 3

async def request_worker(session, queue, endpoint, worker_id):
    while True:
        batch_num, payload = await queue.get()

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.post(endpoint, json=payload, timeout=30) as response:
                    if response.status in (200, 201, 202):
                        break  # ✅ success

                    error_text = await response.text()
                    print(f"❌ [Worker {worker_id}] Batch {batch_num} Failed (Attempt {attempt}) HTTP {response.status}")

            except Exception as e:
                print(f"⚠️ [Worker {worker_id}] Batch {batch_num} Network error (Attempt {attempt}): {e}")

            # Retry delay (basic backoff)
            await asyncio.sleep(2 ** attempt)

        else:
            print(f"🚨 [Worker {worker_id}] Batch {batch_num} permanently failed after {MAX_RETRIES} attempts")

        queue.task_done()
# --- Main ---
async def main():
    print(f"Starting ASYNC ingestion. Concurrency: {MAX_CONCURRENT_REQUESTS}")

    start_time = time.time()
    queue = asyncio.Queue(maxsize=MAX_CONCURRENT_REQUESTS * 2)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        # 🔥 Step 1: Get namespace
        namespace = await create_namespace(session)

        # 🔥 Step 2: Build dynamic endpoints
        insert_endpoint = f"{BASE_URL}/namespaces/{namespace}/batchInsert"
        activate_endpoint = f"{BASE_URL}/namespaces/{namespace}/activate"

        # Start workers
        workers = [
            asyncio.create_task(request_worker(session, queue, insert_endpoint, i))
            for i in range(MAX_CONCURRENT_REQUESTS)
        ]

        # Producer
        batch_payload = {}
        batch_count = 0
        total_inserted = SKIP_ROWS
        rows_scanned = 0

        with open(FILE_PATH, "r", encoding="utf-8") as file:
            reader = csv.reader(file)

            for row in reader:
                rows_scanned += 1

                if rows_scanned <= SKIP_ROWS:
                    continue

                if len(row) < 2:
                    continue

                post_id = row[0].strip()
                score = str(row[1]).strip()
                batch_payload[post_id] = score

                if len(batch_payload) == BATCH_SIZE:
                    batch_count += 1
                    total_inserted += BATCH_SIZE

                    await queue.put((batch_count, {"scores": batch_payload}))
                    batch_payload = {}

                    if batch_count % 100 == 0:
                        elapsed = time.time() - start_time
                        rps = (total_inserted - SKIP_ROWS) / elapsed
                        print(f"Progress: {total_inserted:,} rows | ~{rps:,.0f} rows/sec")

            # Final batch
            if batch_payload:
                batch_count += 1
                total_inserted += len(batch_payload)
                await queue.put((batch_count, {"scores": batch_payload}))

        print("File read complete. Waiting for requests...")
        await queue.join()

        # Stop workers
        for w in workers:
            w.cancel()

        # 🔥 Step 3: Activate
        print("Triggering activation...")
        try:
            async with session.post(activate_endpoint, data="") as response:
                if response.status not in (200, 201, 202):
                    error_text = await response.text()
                    print(f"❌ Activation failed: {error_text}")
                else:
                    print(f"✅ Activation successful!")
        except Exception as e:
            print(f"⚠️ Activation error: {e}")

    total_time = time.time() - start_time
    print(f"\n✅ Done! {total_inserted - SKIP_ROWS:,} rows in {total_time:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())