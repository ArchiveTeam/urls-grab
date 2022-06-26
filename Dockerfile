FROM atdr.meo.ws/archiveteam/grab-base
RUN apt-get update \
  && apt-get install luarocks \
  && luarocks install html-entities
COPY . /grab
RUN ln -fs /usr/local/bin/wget-lua /grab/wget-at
