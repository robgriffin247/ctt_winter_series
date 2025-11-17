import modal
import shlex
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

app_local_path = PROJECT_ROOT / "streamlit" / "app.py"
app_remote_path = "/root/ui/app.py"

image = (
    modal.Image.debian_slim()
    .pip_install("duckdb", "streamlit", "plotly-express", "polars", "emoji")
    .add_local_dir(PROJECT_ROOT / "streamlit", "/root/ui")
    .add_local_file(app_local_path, app_remote_path)
    .add_local_file(PROJECT_ROOT / "streamlit/tabs.py", "/root/ui/tabs.py")
)

app = modal.App("ctt-winter-series-2025-26", image=image)


@app.function(secrets=[modal.Secret.from_name("ctt-secrets")])
@modal.concurrent(max_inputs=50)
@modal.web_server(8000)
def host_web_app():
    cmd = f"streamlit run {app_remote_path} --server.port 8000 --server.enableCORS=false --server.enableXsrfProtection=false"
    subprocess.Popen(cmd, shell=True)