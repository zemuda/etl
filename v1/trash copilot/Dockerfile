FROM python:3.9-slim

# Set the working directory
WORKDIR /etl

# Copy the requirements file
COPY requirements.txt .

# Install the required dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Command to run the pipeline
CMD ["python", "main.py"]