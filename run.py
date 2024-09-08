import uvicorn
import fire
import time
from exchange.utility import settings
from main import app

time.sleep(2)

def start_server(host="0.0.0.0", port=8000 if settings.PORT is None else settings.PORT):
    app.state.port = port
    uvicorn.run("main:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    fire.Fire(start_server)
