import asyncio
import aiohttp
import csv
import time

# --- Configuration ---
FILE_PATH = "/Users/sairakeshreddy/goshposh/locust/mock_scores.csv"
ENDPOINT = "https://stage-entity-score-api.aws.goshd.net/entity-scores/reactive/models/2.0.1/namespaces/69e3d01d342547112a8c7c63/batchInsert"
BATCH_SIZE = 2000

# THE THROTTLE: How many HTTP requests to process at the exact same time.
# Start with 10. If the server handles it easily, try 20. 
MAX_CONCURRENT_REQUESTS = 10 

# RESUME FEATURE: Change this to the number of rows you've already processed 
# if you need to stop the script and restart it later.
SKIP_ROWS = 0 

HEADERS = {
    "Content-Type": "application/json"
    # "x-http-authorization": "Bearer eyJraWQiOiJkODBjOTI1OS0zYjM4LTQ0NDktODRiOS1kMTQ0Y2UwZDAyZjUiLCJhbGciOiJFUzI1NksifQ.eyJleHAiOjE3NzY0MjE3ODR9.KS3lYojb-EDS5G2cGIvtpK2eRELvktMhEVVN15V3rAmo5HmMmrLiThwfPYHpaoBQBPRv3mOBjEWvdItI5ldE7Q"
}

# --- The Worker ---
# Workers constantly pull payloads from the queue and send the HTTP requests concurrently
async def request_worker(session, queue, worker_id):
    while True:
        batch_num, payload = await queue.get()
        try:
            # timeout=30 ensures a hanging server doesn't freeze the worker forever
            async with session.post(ENDPOINT, json=payload, timeout=30) as response:
                if response.status not in (200, 201, 202):
                    error_text = await response.text()
                    print(f"❌ [Worker {worker_id}] Batch {batch_num} Failed! HTTP {response.status}: {error_text}")
        except Exception as e:
            print(f"⚠️ [Worker {worker_id}] Network error on Batch {batch_num}: {e}")
        finally:
            queue.task_done() # Tell the queue this batch is completely finished

# --- The Main Coordinator ---
async def main():
    print(f"Starting ASYNC ingestion. Concurrency level: {MAX_CONCURRENT_REQUESTS}")
    if SKIP_ROWS > 0:
        print(f"Fast-forwarding... Skipping the first {SKIP_ROWS:,} rows. Please wait.")

    start_time = time.time()
    
    # The queue prevents RAM explosion. It only holds a few batches ahead of the workers.
    queue = asyncio.Queue(maxsize=MAX_CONCURRENT_REQUESTS * 2)

    # We use a custom TCP connector to pool connections efficiently
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        # 1. Spin up the background workers
        workers = [asyncio.create_task(request_worker(session, queue, i)) for i in range(MAX_CONCURRENT_REQUESTS)]

        # 2. The Producer: Stream the file and feed the queue
        batch_payload = {}
        batch_count = 0
        total_inserted = SKIP_ROWS # Start counting from where we left off
        rows_scanned = 0

        with open(FILE_PATH, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                rows_scanned += 1
                
                # Instantly skip rows until we reach our starting point
                if rows_scanned <= SKIP_ROWS:
                    continue

                # Skip empty or malformed rows safely
                if len(row) < 2:
                    continue
                
                post_id = row[0].strip()
                score = str(row[1]).strip()
                batch_payload[post_id] = score

                if len(batch_payload) == BATCH_SIZE:
                    batch_count += 1
                    total_inserted += BATCH_SIZE
                    
                    # 'await queue.put' will pause file reading if the workers fall behind, keeping RAM safe!
                    await queue.put((batch_count, {"scores": batch_payload}))
                    batch_payload = {} 

                    if batch_count % 100 == 0:
                        elapsed = time.time() - start_time
                        rps = (total_inserted - SKIP_ROWS) / elapsed
                        print(f"Progress: {total_inserted:,} total rows passed. Speed: ~{rps:,.0f} rows/sec.")

            # Send the final partial batch if any rows are leftover at the end of the file
            if batch_payload:
                batch_count += 1
                total_inserted += len(batch_payload)
                await queue.put((batch_count, {"scores": batch_payload}))

        # 3. Wait for the queue to empty and all workers to finish their current tasks
        print("File read completely. Waiting for final network requests to finish...")
        await queue.join()

        # 4. Shut down the workers cleanly
        for w in workers:
            w.cancel()

    total_time = time.time() - start_time
    print(f"\n✅ Finished! {total_inserted - SKIP_ROWS:,} new rows processed in {total_time:.1f} seconds.")

if __name__ == "__main__":
    # Standard way to execute an asyncio script
    asyncio.run(main())