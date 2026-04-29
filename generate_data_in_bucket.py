# import csv
# import sys

# # --- Configuration ---
# INPUT_FILE = '/Users/sairakeshreddy/goshposh/locust/mock_scores.csv'
# BUCKET1_FILE = '/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket1.csv'
# BUCKET2_FILE = '/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket2.csv'
# BUCKET3_FILE = '/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket3.csv'
# BUCKET4_FILE = '/Users/sairakeshreddy/goshposh/locust/locustfiles/Blocking_Reactive/bucket4.csv'

# TARGET_COUNT = 2500
# BUCKET_COUNT = 4 
# ID_COLUMN_INDEX = 0 

# # --- Java Equivalent Functions ---
# def java_hashcode(s):
#     """Exactly mimics Java's String.hashCode() returning a signed 32-bit int"""
#     if not s:
#         return 0
#     h = 0
#     for c in s:
#         h = (31 * h + ord(c)) & 0xFFFFFFFF
#     # Convert to signed 32-bit integer
#     return h if h < 0x80000000 else h - 0x100000000

# def get_bucket(entity_id):
#     h = java_hashcode(str(entity_id))
#     # Python's modulo operator (%) behaves identically to Java's Math.floorMod() 
#     # when the divisor (BUCKET_COUNT) is positive.
#     bucket = h % BUCKET_COUNT
#     return f"{bucket:02x}"

# # --- Main Extraction Logic ---
# def extract_data():
#     b1_count = 0
#     b2_count = 0
#     b3_count = 0
#     b4_count = 0


#     print("Starting extraction...")

#     with open(INPUT_FILE, 'r', encoding='utf-8') as infile, \
#          open(BUCKET1_FILE, 'w', newline='', encoding='utf-8') as b1_out, \
#          open(BUCKET2_FILE, 'w', newline='', encoding='utf-8') as b2_out, \
#          open(BUCKET3_FILE, 'w', newline='', encoding='utf-8') as b3_out, \
#          open(BUCKET4_FILE, 'w', newline='', encoding='utf-8') as b4_out:

#         reader = csv.reader(infile)
#         b1_writer = csv.writer(b1_out)
#         b2_writer = csv.writer(b2_out)
#         b3_writer = csv.writer(b3_out)
#         b4_writer = csv.writer(b4_out)

#         # Read and write the header row so your output files have correct column names
#         try:
#             headers = next(reader)
#             b1_writer.writerow(headers)
#             b2_writer.writerow(headers)
#             b3_writer.writerow(headers)
#             b4_writer.writerow(headers)
#         except StopIteration:
#             print("Input file is empty!")
#             return

#         # Stream through the 200M rows one at a time
#         for row_num, row in enumerate(reader, start=1):
#             # Stop early if we have found 2500 for both buckets
#             if b1_count >= TARGET_COUNT and b2_count >= TARGET_COUNT and b3_count >= TARGET_COUNT and b4_count >= TARGET_COUNT:
#                 print(f"Success! Reached {TARGET_COUNT} for all buckets.")
#                 break
                
#             if not row: # skip empty lines
#                 continue

#             entity_id = row[ID_COLUMN_INDEX]
#             bucket_hex = get_bucket(entity_id)

#             if bucket_hex == '00' and b1_count < TARGET_COUNT:
#                 b1_writer.writerow(row)
#                 b1_count += 1
#                 if b1_count % 500 == 0:
#                     print(f"Bucket 1 progress: {b1_count}/{TARGET_COUNT}")
                    
#             elif bucket_hex == '01' and b2_count < TARGET_COUNT:
#                 b2_writer.writerow(row)
#                 b2_count += 1
#                 if b2_count % 500 == 0:
#                     print(f"Bucket 2 progress: {b2_count}/{TARGET_COUNT}")

#             elif bucket_hex == '02' and b3_count < TARGET_COUNT:
#                 b3_writer.writerow(row)
#                 b3_count += 1
#                 if b3_count % 500 == 0:
#                     print(f"Bucket 3 progress: {b3_count}/{TARGET_COUNT}")

#             elif bucket_hex == '03' and b4_count < TARGET_COUNT:
#                 b4_writer.writerow(row)
#                 b4_count += 1
#                 if b4_count % 500 == 0:
#                     print(f"Bucket 4 progress: {b4_count}/{TARGET_COUNT}")

#             # Optional: Print progress every 1 million rows so you know it's not frozen
#             if row_num % 1_000_000 == 0:
#                 print(f"Scanned {row_num:,} rows...")

#     print("\n--- Final Status ---")
#     print(f"Saved {b1_count} IDs to {BUCKET1_FILE}")
#     print(f"Saved {b2_count} IDs to {BUCKET2_FILE}")
#     print(f"Saved {b3_count} IDs to {BUCKET3_FILE}")
#     print(f"Saved {b4_count} IDs to {BUCKET4_FILE}")

# if __name__ == '__main__':
#     extract_data()





import csv
from collections import defaultdict

# --- Configuration ---
INPUT_FILE = '/Users/sairakeshreddy/goshposh/locust/mock_scores.csv'
BUCKET_COUNT = 6
ID_COLUMN_INDEX = 0

# --- Java Equivalent Functions ---
def java_hashcode(s):
    if not s:
        return 0
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return h if h < 0x80000000 else h - 0x100000000

def get_bucket(entity_id):
    h = java_hashcode(str(entity_id))
    bucket = h % BUCKET_COUNT
    return f"{bucket:02x}"

# --- Main Counting Logic ---
def count_buckets():
    bucket_counts = defaultdict(int)
    total_rows = 0

    print("Starting full file scan for bucket distribution...")

    with open(INPUT_FILE, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile)

        # Skip header
        try:
            next(reader)
        except StopIteration:
            print("File is empty!")
            return

        for row_num, row in enumerate(reader, start=1):
            if not row:
                continue

            entity_id = row[ID_COLUMN_INDEX]
            bucket_hex = get_bucket(entity_id)

            bucket_counts[bucket_hex] += 1
            total_rows += 1

            # Progress log (important for huge files like 200M rows)
            if row_num % 1_000_000 == 0:
                for bucket in sorted(bucket_counts.keys()):
                    print(f"Bucket {bucket}: {bucket_counts[bucket]:,}")
                print(f"Scanned {row_num:,} rows...")

    # --- Results ---
    print("\n--- Bucket Distribution ---")
    for bucket in sorted(bucket_counts.keys()):
        print(f"Bucket {bucket}: {bucket_counts[bucket]:,}")

    print(f"\nTotal rows processed: {total_rows:,}")

if __name__ == '__main__':
    count_buckets()
