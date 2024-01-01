import requests
import platform
import time
from threading import Lock, Thread


lock = Lock()
server = "http://localhost:5279"
data_to_download = 0
total_size = 0
claims = []
active_claim_indexes = []

channel = input("Enter channel: ")
quota = int(input("Enter max download amount in MB: ")) * \
    1000000  # Convert to bytes
max_threads = int(input("Max parallel downloads: "))

print("Getting claims:....")
page = 1
claims_left = True
last_height = 99999999999999  # high enough default value
quota_full = False
while claims_left:
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
            "creation_height": "<=%i" % last_height,
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


def isDownloaded(i):
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


def printDownloadStatus():
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


count = 0
finished_claims = 0


def download():
    global count
    global finished_claims
    global active_claim_indexes
    try:
        while count < len(claims):
            lock.acquire()
            my_count = count
            active_claim_indexes.append(my_count)
            try:
                claim = claims[my_count]
            except IndexError:
                print("No more new claims")
                active_claim_indexes.remove(my_count)
                lock.release()
                break
            count += 1
            lock.release()
            response = requests.post(server, json={
                "method": "get",
                "params": {
                    "uri": claim.get("url"),
                }}).json()

            # Start download(Respects daemon settigs about saving the file)
            requests.get(response["result"]["streaming_url"])
            active_claim_indexes.remove(my_count)
            finished_claims += 1
    except KeyboardInterrupt:
        exit()


for i in range(len(claims)):
    # Set count to first non-downloaded publish
    if not isDownloaded(i):
        count = i
        finished_claims = i
        break
    if i == len(claims)-1:
        print("Channel is downloaded")
        exit()

# Start downloads
thread_count = 0
while thread_count < max_threads:
    thread_count += 1
    Thread(target=download).start()

while len(active_claim_indexes) != 0 or finished_claims < len(claims):
    print("Downloaded: %d/%d" % (finished_claims, len(claims)), end='\r')
    # printDownloadStatus()
    time.sleep(1)
