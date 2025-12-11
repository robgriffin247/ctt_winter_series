import modal
import shlex
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

app_local_path = PROJECT_ROOT / "streamlit" / "app.py"
app_remote_path = "/root/ui/app.py"

image = (
    modal.Image.debian_slim()
    .pip_install("duckdb", "streamlit", "plotly-express", "polars", "emoji", "pycountry")
    .add_local_dir(PROJECT_ROOT / "streamlit", "/root/ui")
    .add_local_dir(PROJECT_ROOT / ".streamlit", "/root/.streamlit")
    .add_local_file(app_local_path, app_remote_path)
    .add_local_file(PROJECT_ROOT / "streamlit/tabs.py", "/root/ui/tabs.py")
    .add_local_file(PROJECT_ROOT / ".streamlit/config.toml", "/root/.streamliy/config.toml")
)

app = modal.App("ctt-winter-series-2025-26", image=image)


@app.function(secrets=[modal.Secret.from_name("ctt-secrets")])
@modal.concurrent(max_inputs=20)
@modal.web_server(8000)
def host_web_app():
    cmd = f"streamlit run {app_remote_path} --server.port 8000 --server.enableCORS=false --server.enableXsrfProtection=false"
    subprocess.Popen(cmd, shell=True)







# import modal
# import subprocess
# from pathlib import Path

# PROJECT_ROOT = Path(__file__).resolve().parents[1]

# app_local_path = PROJECT_ROOT / "streamlit" / "app.py"
# app_remote_path = "/root/ui/app.py"

# image = (
#     modal.Image.debian_slim()
#     .pip_install("duckdb", "streamlit", "plotly-express", "polars", "emoji", "pycountry")
#     .add_local_dir(PROJECT_ROOT / "streamlit", "/root/ui")
#     .add_local_dir(PROJECT_ROOT / ".streamlit", "/root/.streamlit")
#     .add_local_file(app_local_path, app_remote_path)
#     .add_local_file(PROJECT_ROOT / "streamlit/tabs.py", "/root/ui/tabs.py")
#     .add_local_file(PROJECT_ROOT / ".streamlit/config.toml", "/root/.streamlit/config.toml")
# )

# app = modal.App("ctt-winter-series-2025-26", image=image)

# @app.function(
#     secrets=[modal.Secret.from_name("ctt-secrets")],
#     memory=256,  # Match Fly: 512MB
#     cpu=1,     # Match Fly: 1 shared CPU
#     timeout=120,  # Match Fly timeout: 30 minutes (if you uncomment that block)
#     min_containers=0,  # Match Fly min_machines_running=0 (scale to zero)
#     max_containers=3,
#     scaledown_window=120,  # Match Fly: 30 min before shutdown
# )
# @modal.web_server(8000, startup_timeout=60)
# def host_web_app():
#     subprocess.Popen([
#         "streamlit", "run", app_remote_path,
#         "--server.port", "8000",
#         "--server.address", "0.0.0.0",
#         "--server.enableCORS", "false",
#         "--server.enableXsrfProtection", "false",
#         "--server.headless", "true",
#     ])




# @app.function(
#     secrets=[modal.Secret.from_name("ctt-secrets")],
#     memory=512,  # 4GB - adjust based on your data size
#     cpu=2.0,  # 2 CPUs for handling concurrent users
#     timeout=300,  # 5 minutes for long-running sessions
#     scaledown_window=600,  # Keep containers alive 10 min after last request
# )
# @modal.concurrent(max_inputs=1000)
# @modal.web_server(8000, startup_timeout=60)
# def host_web_app():
#     # Run streamlit without shell=True for better process control
#     subprocess.Popen([
#         "streamlit", "run", app_remote_path,
#         "--server.port", "8000",
#         "--server.address", "0.0.0.0",
#         "--server.enableCORS", "false",
#         "--server.enableXsrfProtection", "false",
#         "--server.headless", "true",
#     ])
