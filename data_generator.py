import socket, time, json, random, threading, uuid, datetime

HOST = '127.0.0.1'
PORT = 9999
INTERVAL = 1.0

def make_record():
    return {
        "trip_id": str(uuid.uuid4()),
        "driver_id": "driver_" + str(random.randint(1,5)),
        "distance_km": round(random.uniform(0.5, 25.0), 2),
        "fare_amount": round(random.uniform(3.0, 75.0), 2),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
    }

def handle_client(conn, addr):
    print(f"Client connected: {addr}")
    try:
        while True:
            record = make_record()
            conn.sendall((json.dumps(record) + "\n").encode("utf-8"))
            time.sleep(INTERVAL)
    except Exception as e:
        print("Connection closed:", e)
    finally:
        conn.close()

def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(5)
    print(f"Data generator running on {HOST}:{PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == '__main__':
    main()
