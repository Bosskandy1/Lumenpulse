FROM python:3.9-slim

WORKDIR /app

ENV HF_HOME=/app/.cache/huggingface
ENV TRANSFORMERS_CACHE=/app/.cache/huggingface/hub

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Bake FinBERT weights into the image so containers can run without Hub access at runtime.
RUN mkdir -p /app/.cache/huggingface \
    && python -c "from transformers import AutoModelForSequenceClassification, AutoTokenizer; m='ProsusAI/finbert'; AutoTokenizer.from_pretrained(m); AutoModelForSequenceClassification.from_pretrained(m)"

# Vendor the pinned spaCy NER model at build time (not container start). The
# script installs the exact pinned wheel and copies it into /app/models/ner so
# the runtime has zero outbound network dependency for model resolution.
# Fails the build if the pinned model cannot be fetched.
RUN python scripts/fetch_ner_model.py && python scripts/fetch_ner_model.py --check-only

CMD ["python", "src/main.py"]
