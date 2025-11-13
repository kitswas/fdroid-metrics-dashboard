# Alpine Linux base image
FROM alpine

# Install git, curl, coreutils; then install uv
RUN apk --no-cache add git curl coreutils
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# git clone
RUN git clone --depth 1 https://github.com/kitswas/fdroid-metrics-dashboard.git

# Remove unnecessary packages
RUN apk --no-cache del git curl

# Set working directory to the cloned repo
WORKDIR /fdroid-metrics-dashboard

# Install dependencies (Reduces startup time at the cost of a larger image)
RUN uv sync --link-mode=symlink

# Get 12 months of data (Reduces need to fetch data later at the cost of a larger image)
RUN apk --no-cache add coreutils \
&& uv run --link-mode=symlink python -m etl.getdata_search --start $(date -d '12 months ago' +%Y-%m-%d) --end $(date +%Y-%m-%d) \
&& uv run --link-mode=symlink python -m etl.getdata_apps --start $(date -d '12 months ago' +%Y-%m-%d) --end $(date +%Y-%m-%d) \
&& apk --no-cache del coreutils

# Enabling both the above steps increases the image size from ~100MB to ~1GB

# Expose port 8080
EXPOSE 8080

# Start the application
CMD ["uv", "run", "--link-mode=symlink", "streamlit", "run", "dashboard.py", "--logger.level=info", "--server.port", "8080"]
