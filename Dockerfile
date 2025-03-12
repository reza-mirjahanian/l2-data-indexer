# Use the Golang image as the base image
FROM golang:1.22

# Install necessary build dependencies (if needed)
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    git

# Set the working directory inside the container
WORKDIR /app

# Copy your Go application into the Docker image

# Copy the Go module files
COPY . .

RUN go mod download

# Install Go modules if needed
RUN go get github.com/boltdb/bolt/...

# Set the default command to run a shell
CMD ["/bin/bash"]
