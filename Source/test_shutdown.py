import subprocess
import sys
import time
import signal
import os

# Start the server process
proc = subprocess.Popen(
    [sys.executable, "-m", "server"],
    cwd=r"C:\Users\Mrs Phiri\Desktop\IoT Telemetry\Source",
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    bufsize=1
)

# Let it run for 4 seconds
time.sleep(4)

# Send Ctrl+C via SIGINT (on Windows, this is done via GenerateConsoleCtrlEvent)
try:
    os.kill(proc.pid, signal.SIGINT)
except Exception as e:
    print(f"Error sending SIGINT: {e}")
    proc.terminate()

# Wait for shutdown and collect output
try:
    stdout, _ = proc.communicate(timeout=5)
    print(stdout)
except subprocess.TimeoutExpired:
    print("Process did not shut down within timeout, killing...")
    proc.kill()
    stdout, _ = proc.communicate()
    print(stdout)
