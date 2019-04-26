FROM nodesource/node:8.11.1

ADD package.json package.json
RUN npm install
ADD . .

CMD ["npm","test"]
