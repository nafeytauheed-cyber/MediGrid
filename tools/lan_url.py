"""Print the LAN URL of this machine for other devices on the same Wi-Fi. Standard library only; run on the host:
    python tools/lan_url.py
"""
import socket


def lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # No packet is sent: connect() on UDP only selects the outgoing interface.
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


if __name__ == "__main__":
    ip = lan_ip()
    print(f"MediGrid on this LAN: http://{ip}:8080/control")
    print(f"Health check:          http://{ip}:8080/healthz")
    if ip.startswith("127."):
        print("No LAN interface found. Connect to Wi-Fi or a phone hotspot.")
