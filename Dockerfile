FROM python:3.10-slim as base

ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1


FROM base AS deps
RUN pip install -U pip setuptools wheel pipenv

COPY Pipfile .
COPY Pipfile.lock .
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy


FROM base AS runtime
COPY --from=deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

WORKDIR /code
COPY . .

ENTRYPOINT ["python", "main.py"]