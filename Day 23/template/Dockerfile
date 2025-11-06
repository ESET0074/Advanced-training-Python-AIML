# Use official Python image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy dependency list first (helps in caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of the project
COPY . /app

# Train the model to generate iris_model.pkl
RUN python train.py

# Expose port for FastAPI
EXPOSE 8000

# Start FastAPI server
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
