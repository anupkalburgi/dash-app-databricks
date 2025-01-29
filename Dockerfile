# Base image
FROM python:3.12-slim-bullseye



# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Set working directory
WORKDIR /app

# Copy the application files
COPY . /app

# Create virtual environment
# Expose the port for the FastAPI app
EXPOSE 8080

# Command to run the application
CMD ["sh", "./run.sh"]
