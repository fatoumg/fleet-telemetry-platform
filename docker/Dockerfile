# One image, two roles: the API and the simulator.
#
# They share every dependency and every line of the source tree, so building two images would
# just be two things to keep in step. The compose file picks the role with `command`.
#
# Build context is the PROJECT ROOT, not this directory:
#     docker compose -f docker/docker-compose.yml build

FROM python:3.13-slim

# PYTHONUNBUFFERED matters more than it looks: without it Python block-buffers stdout when it
# is a pipe rather than a terminal, so `docker logs` shows nothing for minutes and a perfectly
# healthy simulator looks hung.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /opt/fleet

# Dependencies in their own layer, before the application code. Editing app/ or simulator/
# then rebuilds in seconds; without the split, every code change reinstalls FastAPI.
#
# README.md is here because pyproject.toml declares it as the project readme, and the build
# fails without it. psycopg[binary] bundles its own libpq, so there is no apt-get step.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[app]"

COPY app ./app
COPY simulator ./simulator

# Neither role needs root, and neither writes to disk.
USER nobody

# Overridden by the api service. The default is the simulator because that is the one that is
# meant to just keep running.
CMD ["python", "-m", "simulator", "--forever"]
