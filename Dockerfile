# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install ffmpeg for video processing
RUN apt-get update && apt-get install -y ffmpeg && apt-get clean

# Make port 5001 available to the world outside this container
EXPOSE 5001

# Run the Flask app on port 5001
CMD ["python", "app.py", "--host=0.0.0.0", "--port=5001"]
