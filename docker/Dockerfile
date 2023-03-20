FROM python:3.10-slim
EXPOSE 3000
COPY ./ fake-star-detector/
WORKDIR /fake-star-detector
RUN ls -lah
RUN pip install -e ".[dev]"
CMD ["dagster", "dev", "-h", "0.0.0.0"]