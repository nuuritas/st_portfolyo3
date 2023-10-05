import subprocess

subprocess.run(["python", "python/midas_raw_create.py"])
print("midas_raw_create.py completed")
subprocess.run(["python", "python/midas_create_summary.py"])
print("midas_create_summary.py completed")