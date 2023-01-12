FROM node:18-alpine

WORKDIR /code/

COPY package.json /code/
COPY package-lock.json /code/

RUN yarn install

COPY . /code/

CMD ["node", "run.js"]