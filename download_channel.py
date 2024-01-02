import requests
import time
from threading import Lock, Thread


lock = Lock()
server = "http://localhost:5279"
data_to_download = 0
total_size = 0
claims = []
active_claim_indexes = []

def get_download_info():
    global channel
    global quota
    global max_threads
    channel = input("Enter channel: ")
    quota = int(input("Enter max download amount in MB: ")) * \
        1000000  # Convert to bytes
    max_threads = int(input("Max parallel downloads: "))

def get_claims_to_download():
    print("Getting claims:....")
    global total_size
    global data_to_download
    page = 1
    claims_left = True
    last_height = 99999999999999  # high enough default value
    quota_full = False
    while claims_left and not quota_full:
        result = requests.post(server, json={
            "method": "claim_search",
            "params":
            {
                "channel": channel,
                "claim_type": "stream",
                "page_size": 50,
                "has_source": True,
                "order_by": "creation_height",
                # This allows to query >1000 claims (May cause some duplicates, but shouldn't matter)
                "creation_height": f"""<={last_height}""",
                "page": page
            }
        }).json()["result"]

        if result["total_items"] == 0:
            print("Channel not found")
            exit(1)

        for item in result["items"]:
            url = item["permanent_url"]
            claim_id = item["claim_id"]
            if page == 20:
                last_height = item["meta"]["creation_height"]

            # Claim may not have size reported
            try:
                if not quota_full:
                    size = int(item["value"]["source"]["size"])
            except KeyError:
                if input("Unknown size, claim: %s\nDownload anywy(y/N)" % item["name"]).lower() == 'y':
                    claims.append({"url": url, "claim_id": claim_id})
                continue

            total_size += size
            if data_to_download + size < quota:
                data_to_download += size
                claims.append({"url": url, "claim_id": claim_id})
            else:
                quota_full = True

        print("%i " % len(claims), end="\r")
        # 20 is current maximum page number (this is why creation height is used)
        if page != 20:
            page += 1
        else:
            # If results filled all 20 pages, there may be more --> Re-query
            claims_left = result["total_pages"] == 20
            page = 1

def is_downloaded(i):
    try:
        result = requests.post(server, json={
            "method": "file_list",
            "params": {
                "claim_id": claims[i].get("claim_id")
            }
        }).json()["result"]
        blobs_completed = int(result["items"][0]["blobs_completed"])
        blobs_in_stream = int(result["items"][0]["blobs_in_stream"])
        if blobs_completed == blobs_in_stream:
            print("Finished download for claim no %i: %s" %
                  (i, claims[i]["url"]))
            return True
    except IndexError:
        pass
    return False

last_finished_blobs_count = None
last_time = None
# Simple, but not very accurate
def get_current_download_speed():
    global last_finished_blobs_count
    global last_time
    response = requests.post(server, json={"method": "status"}).json()
    finished_blobs_count = response["result"]["blob_manager"]["finished_blobs"]

    if last_finished_blobs_count is None or last_time is None:
        last_finished_blobs_count = finished_blobs_count
        last_time = time.time()
        return 0

    delta_blobs_count =  finished_blobs_count - last_finished_blobs_count
    last_finished_blobs_count = finished_blobs_count

    now = time.time()
    delta_time = now - last_time
    last_time = now

    data_downloaded = delta_blobs_count * 2.048
    return (data_downloaded / delta_time)


def print_download_status():
    output = ""
    global active_claim_indexes
    try:
        for i in active_claim_indexes:
            result = requests.post(server, json={
                "method": "file_list",
                "params": {
                    "claim_id": claims[i].get("claim_id")
                }
            }).json()["result"]
            blobs_completed = int(result["items"][0]["blobs_completed"])
            blobs_in_stream = int(result["items"][0]["blobs_in_stream"])
            print("Blobs completeted: %d" % blobs_completed)
            output += "Downloading file %d/%d Blobs_in_file: %d/%d   \n" % (
                (i + 1),
                len(claims),
                blobs_completed,
                blobs_in_stream)
    except IndexError:
        pass
    print(output)

next_download_index = 0
finished_claims_count = 0
def download():
    global next_download_index
    global finished_claims_count
    global active_claim_indexes
    try:
        while next_download_index < len(claims):
            lock.acquire()
            current_download_index = next_download_index
            active_claim_indexes.append(current_download_index)
            try:
                claim = claims[current_download_index]
            except IndexError:
                print("No more new claims")
                active_claim_indexes.remove(current_download_index)
                lock.release()
                break
            next_download_index += 1
            lock.release()
            response = requests.post(server, json={
                "method": "get",
                "params": {
                    "uri": claim.get("url"),
                }}).json()

            # Start download(Respects daemon settigs about saving the file)
            requests.get(response["result"]["streaming_url"])
            active_claim_indexes.remove(current_download_index)
            finished_claims_count += 1
    except KeyboardInterrupt:
        return

def get_first_non_downloaded_claim_index():
    global next_download_index
    global finished_claims_count
    for i in range(len(claims)):
        # Set next_download_index to first non-downloaded publish
        if not is_downloaded(i):
            next_download_index = i
            finished_claims_count = i
            break
        if i == len(claims)-1:
            print("Channel is downloaded")
            exit()

def start_downloads():
    global max_threads
    # Print info and ask confirmation
    print("\nClaims to be downloaded: ")
    for i in claims:
        print(i.get("url"))
    print("Total(Large channels may have some duplicates, these will be downloaded only ones): %s\n" % (len(claims)))
    print("Known data on channel: %d / %d MB: " %
        (data_to_download/1000000, total_size/1000000))
    print("Download threads: %i " % max_threads)
    if input("Start Download(y/N): ").lower() != "y":
        print("Exiting")
        exit(0)

    max_threads = min(len(claims) - finished_claims_count, max_threads)
    thread_count = 0
    while thread_count < max_threads:
        thread_count += 1
        Thread(target=download).start()

    while len(active_claim_indexes) != 0 or finished_claims_count < len(claims):
        print(f"""Downloaded: {finished_claims_count}/{len(claims)} (~{get_current_download_speed():.2f} MB/s)""", end='\r')
        # print_download_status()
        time.sleep(1)

def main():
    try:
        get_download_info()
        get_claims_to_download()
        get_first_non_downloaded_claim_index()
        start_downloads()
    except KeyboardInterrupt:
        print()
        exit()

main()
