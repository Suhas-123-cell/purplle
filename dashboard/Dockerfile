FROM nginx:alpine

# Remove default nginx static assets
RUN rm -rf /usr/share/nginx/html/*

# Copy dashboard
COPY index.html /usr/share/nginx/html/index.html

EXPOSE 80
