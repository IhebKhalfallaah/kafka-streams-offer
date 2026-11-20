# ===== BUILD STAGE =====
FROM node:22-alpine AS build
WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build

# ===== RUNTIME STAGE =====
FROM nginx:alpine
# copy built static files to nginx default root
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
