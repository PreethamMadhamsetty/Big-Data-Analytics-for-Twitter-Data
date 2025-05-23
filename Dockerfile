# Use the bitnami/spark image as the base image
FROM bitnami/spark:latest

# Copy the requirements.txt file into the image
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies using pip
RUN pip install --no-cache-dir -r /tmp/requirements.txt
