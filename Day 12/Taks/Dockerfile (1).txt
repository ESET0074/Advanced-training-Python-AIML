FROM python:3.12-alpine
# Install GCC build dependency
RUN apk update && \
    apk add --no-cache gcc g++ musl-dev
# Set the working directory
WORKDIR /app

# Copy only the required files
COPY requirements.txt .
# RUN pip install uv
# RUN uv pip install --system -r requirements.txt
# RUN python -m spacy download en_core_web_sm
RUN pip install --no-cache-dir nltk==3.6.5
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
#Install only NLTK (or just directly use the version you need)
# RUN pip install --no-cache-dir nltk==3.6.5


# Run your script
# CMD ["python", "01_sentiment_analysis.py"]