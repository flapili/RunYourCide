import asyncio
from pathlib import Path
import tempfile

import aiodocker
from pydantic import BaseModel, ValidationError
from fastapi import FastAPI, WebSocket, status


class JobSubmissionFile(BaseModel):
    name: str
    content: str


class JobSubmission(BaseModel):
    image: str
    files: list[JobSubmissionFile]

    cmd: list[str]
    env: dict[str, str] = {}


app = FastAPI()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        async with asyncio.timeout(1):
            msg = await websocket.receive_json()
    except asyncio.TimeoutError:
        await websocket.close(
            code=status.WS_1001_GOING_AWAY,
            reason="Timeout on waiting for a message",
        )
        return

    try:
        job = JobSubmission(**msg)
    except ValidationError as e:
        await websocket.send_json({"error": e.errors()})
        return

    docker = aiodocker.Docker()

    with tempfile.TemporaryDirectory(prefix="code-runner-jobs-") as tempdir:
        p = Path(tempdir)
        for file in job.files:
            (p / file.name).write_text(file.content)

        container = await docker.containers.run(
            config={
                "WorkingDir": "/code",
                "Cmd": job.cmd,
                "Image": job.image,
                "Env": [f"{k}={v}" for k, v in job.env.items()],
                "AttachStdin": False,
                "AttachStdout": False,
                "AttachStderr": False,
                "Tty": False,
                "OpenStdin": False,
                "HostConfig": {
                    "AutoRemove": True,
                    "Binds": [f"{p}:/code"],
                },
            }
        )
        from typing import AsyncGenerator, Literal

        async def stdio_handler(
            stream: AsyncGenerator[str, None], type: Literal["stdout", "stderr"]
        ):
            async for line in stream:
                await websocket.send_json({"type": "data", "stream": type, "text": line})

        await asyncio.gather(
            stdio_handler(
                container.log(follow=True, stdout=True, since=0, until=0),
                "stdout",
            ),
            stdio_handler(
                container.log(follow=True, stderr=True, since=0, until=0),
                "stderr",
            ),
        )

    res = await container.wait()
    await websocket.send_json({"type": "exit", "code": res["StatusCode"]})
    await docker.close()
